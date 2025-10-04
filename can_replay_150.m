function can_replay_150(csv_file, varargin)
% CAN_REPLAY_150 — simple fixed-rate (150 Hz) SLCAN CSV replayer
% Requires columns: ID_hex, DLC, b0..b7       (no timestamps)
%
% Examples:
%   can_replay_150('mylog.csv');                      % COM5 @ 921600, 1 Mbps, 150 Hz
%   can_replay_150('mylog.csv','PORT',"COM7");        % pick port
%   can_replay_150('mylog.csv','RateHz',200);         % different fixed rate
%   can_replay_150('mylog.csv','DryRun',true);        % preview first frames only

    % ---------- Options ----------
    p = inputParser;
    addRequired(p,'csv_file',@(s)ischar(s)||isstring(s));
    addParameter(p,'PORT',"COM5");
    addParameter(p,'UART_BAUD',921600,@(x)isnumeric(x)&&isscalar(x));
    addParameter(p,'CAN_RATE',"S8",@(s)ischar(s)||isstring(s));   % 1 Mbps default
    addParameter(p,'RateHz',150,@(x)isnumeric(x)&&isscalar(x)&&x>0);
    addParameter(p,'Loop',1,@(x)isnumeric(x)&&isscalar(x)&&x>=1);
    addParameter(p,'IDs',strings(0,1),@(x)isstring(x)||iscellstr(x)); % optional filter
    addParameter(p,'DryRun',false,@islogical);
    parse(p,csv_file,varargin{:});
    o = p.Results;

    % ---------- Load CSV ----------
    T = readtable(char(o.csv_file), 'TextType','string');

    % Required columns
    need = ["ID_hex","DLC","b0","b1","b2","b3","b4","b5","b6","b7"];
    for n = need
        if ~ismember(n, T.Properties.VariableNames)
            error('CSV missing required column: %s', n);
        end
    end

    % Optional fast-path if DATA_hex provided (not required)
    hasDATA = ismember("DATA_hex",T.Properties.VariableNames);

    % Optional ID filter
    if ~isempty(o.IDs)
        T = T(ismember(upper(strtrim(T.ID_hex)), upper(string(o.IDs))), :);
    end
    if isempty(T)
        error('No rows to replay after filtering.');
    end

    % ---------- Build TX lines (classic CAN: use "t" + 11-bit ID) ----------
    % If an ID is longer than 3 hex nibbles, we'll still send as extended using "T".
    N  = height(T);
    TX = strings(N,1);

    for i=1:N
        idhex = cleanHex(T.ID_hex(i));
        dlc   = uint8(limit(toNum(T.DLC(i)),0,8));

        % DATA: prefer DATA_hex if present & non-empty; else build from b0..b7 up to DLC
        data = "";
        if hasDATA
            dh = strtrim(T.DATA_hex(i));
            if dh~="" && ~ismissing(dh)
                data = upper(regexprep(dh,'\s+',''));
            end
        end
        if data=="" && dlc>0
            bytes = strings(1,dlc);
            for b=1:double(dlc)
                bytes(b) = sprintf('%02X', uint8(limit(toNum(T.("b"+(b-1))(i)),0,255)));
            end
            data = join(bytes,"");
        end

        % Decide standard vs extended purely by ID length (3 nibbles => std)
        if strlength(idhex) <= 3
            id3 = pad(idhex,3,'left','0');
            msg = "t" + id3 + sprintf('%1X',dlc) + data;
        else
            id8 = pad(idhex,8,'left','0');
            msg = "T" + id8 + sprintf('%1X',dlc) + data;
        end

        TX(i) = msg + string(char(13)); % append CR
    end

    fprintf('Loaded "%s": %d frames (fixed %.1f Hz)\n', char(o.csv_file), N, o.RateHz);

    % ---------- DryRun preview ----------
    if o.DryRun
        k = min(10,N);
        fprintf('DryRun preview (first %d TX lines):\n',k);
        for i=1:k
            disp(stripCR(TX(i)));
        end
        return;
    end

    % ---------- Serial open & simple SLCAN init ----------
    s = [];
    try
        s = serialport(string(o.PORT), o.UART_BAUD);
        s.Timeout = 1.0;
        configureTerminator(s,"CR");
        flush(s);

        sendCmd(s,"C","Close bus");
        sendCmd(s,string(o.CAN_RATE),"Set bit rate");
        sendCmd(s,"m00000000","Set acc code");
        sendCmd(s,"M00000000","Set acc mask");
        sendCmd(s,"Z0","Timestamps OFF");
        sendCmd(s,"O","Open bus");

        % Sanity TX
        sendLine(s, TX(1));
        fprintf('Sanity TX OK: %s\n', stripCR(TX(1)));

        % ---------- Fixed-rate replay ----------
        per = 1 / o.RateHz;
        for pass = 1:o.Loop
            fprintf('Pass %d/%d...\n', pass, o.Loop);
            t0 = tic;
            for i=1:N
                sendLine(s, TX(i));
                % keep uniform spacing referenced to start time (not cumulative)
                target = (i) * per;
                dt = target - toc(t0);
                if dt > 0, pause(dt); end
            end
        end

        sendCmd(s,"C","Close bus");
        try, clear s; catch, end
        fprintf('Done.\n');

    catch ME
        if ~isempty(s)
            try, sendCmd(s,"C","Close bus"); catch, end
            try, clear s; catch, end
        end
        rethrow(ME);
    end
end

% ===== Helpers =====
function h = cleanHex(x)
    s = upper(strtrim(string(x)));
    s = strrep(strrep(s,"0X",""),"0x","");
    if s=="" || ~all(isstrprop(s,'xdigit'))
        error('Bad ID_hex: "%s"', s);
    end
    h = s;
end
function v = toNum(x)
    if isnumeric(x), v = double(x); return; end
    v = str2double(strtrim(string(x)));
    if isnan(v), v = 0; end
end
function y = limit(x,lo,hi), y = min(max(x,lo),hi); end
function sendCmd(s,cmd,desc)
    writeline(s, cmd);
    % try to read echo briefly; ignore if none
    t0 = tic; resp = "";
    while toc(t0) < 0.05
        if s.NumBytesAvailable>0, resp = strtrim(readline(s)); break; end
    end
    if resp==""; fprintf('%-18s : (no-echo)\n', desc);
    else         fprintf('%-18s : %s\n', desc, resp); end
end
function sendLine(s, lineWithCR)
    msg = stripCR(lineWithCR);
    write(s, uint8(char(msg)), "uint8");
    write(s, uint8(13), "uint8"); % CR
end
function out = stripCR(str)
    out = str;
    if endsWith(out, string(char(13)))
        out = extractBefore(out, strlength(out));
    end
end
