function can_replay_150(varargin)
% CAN_REPLAY_150 — simple fixed-rate (150 Hz) SLCAN CSV replayer
% Supports multiple CSV files played in sequence
%
% Examples:
%   can_replay_150('mylog.csv');                                   % single file
%   can_replay_150('file1.csv','file2.csv','file3.csv');          % sequence
%   can_replay_150('file1.csv','file2.csv','PORT',"COM7");        % with options
%   can_replay_150('file1.csv','RateHz',200);                     % different rate
%   can_replay_150('mylog.csv','DryRun',true);                    % preview

    % ---------- Parse inputs ----------
    % Separate CSV files from name-value parameters
    csv_files = {};
    params = {};
    
    param_names = {'PORT','UART_BAUD','CAN_RATE','RateHz','Loop','IDs','DryRun'};
    
    i = 1;
    while i <= length(varargin)
        arg = varargin{i};
        
        is_param = false;
        % Check if it's a parameter name (must match exactly)
        if ischar(arg) || isstring(arg)
            arg_str = char(arg);
            if any(strcmpi(arg_str, param_names))
                % This is a parameter name
                params{end+1} = varargin{i};
                if i < length(varargin)
                    params{end+1} = varargin{i+1};
                    i = i + 2;
                else
                    i = i + 1;
                end
                is_param = true;
            end
        end
        
        % If not a parameter, treat as CSV filename
        if ~is_param
            if (ischar(arg) || isstring(arg))
                csv_files{end+1} = char(arg);
            end
            i = i + 1;
        end
    end
    
    if isempty(csv_files)
        error('No CSV files specified');
    end
    
    fprintf('Sequence: %d CSV file(s)\n', length(csv_files));
    for k = 1:length(csv_files)
        fprintf('  %d. %s\n', k, csv_files{k});
    end

    % ---------- Options ----------
    p = inputParser;
    addParameter(p,'PORT',"COM5");
    addParameter(p,'UART_BAUD',921600,@(x)isnumeric(x)&&isscalar(x));
    addParameter(p,'CAN_RATE',"S8",@(s)ischar(s)||isstring(s));
    addParameter(p,'RateHz',150,@(x)isnumeric(x)&&isscalar(x)&&x>0);
    addParameter(p,'Loop',1,@(x)isnumeric(x)&&isscalar(x)&&x>=1);
    addParameter(p,'IDs',strings(0,1),@(x)isstring(x)||iscellstr(x));
    addParameter(p,'DryRun',false,@islogical);
    parse(p,params{:});
    o = p.Results;

    % ---------- Load all CSV files and concatenate ----------
    all_TX = strings(0,1);
    
    for file_idx = 1:length(csv_files)
        csv_file = csv_files{file_idx};
        fprintf('\nLoading file %d/%d: %s\n', file_idx, length(csv_files), csv_file);
        
        T = readtable(csv_file, 'TextType','string');

        % Required columns
        need = ["ID_hex","DLC","b0","b1","b2","b3","b4","b5","b6","b7"];
        for n = need
            if ~ismember(n, T.Properties.VariableNames)
                error('CSV missing required column: %s', n);
            end
        end

        % Optional fast-path if DATA_hex provided
        hasDATA = ismember("DATA_hex",T.Properties.VariableNames);

        % Optional ID filter
        if ~isempty(o.IDs)
            T = T(ismember(upper(strtrim(T.ID_hex)), upper(string(o.IDs))), :);
        end
        if isempty(T)
            fprintf('  Warning: No rows after filtering in %s\n', csv_file);
            continue;
        end

        % ---------- Build TX lines ----------
        N = height(T);
        TX = strings(N,1);

        for i=1:N
            idhex = cleanHex(T.ID_hex(i));
            dlc   = uint8(limit(toNum(T.DLC(i)),0,8));

            % DATA
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

            % Standard vs extended
            if strlength(idhex) <= 3
                id3 = pad(idhex,3,'left','0');
                msg = "t" + id3 + sprintf('%1X',dlc) + data;
            else
                id8 = pad(idhex,8,'left','0');
                msg = "T" + id8 + sprintf('%1X',dlc) + data;
            end

            TX(i) = msg + string(char(13));
        end
        
        fprintf('  Loaded %d frames\n', N);
        all_TX = [all_TX; TX];
    end
    
    N_total = length(all_TX);
    fprintf('\nTotal frames in sequence: %d (%.1f seconds @ %.1f Hz)\n', ...
        N_total, N_total/o.RateHz, o.RateHz);

    % ---------- DryRun preview ----------
    if o.DryRun
        k = min(10,N_total);
        fprintf('\nDryRun preview (first %d TX lines):\n',k);
        for i=1:k
            disp(stripCR(all_TX(i)));
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
        sendLine(s, all_TX(1));
        fprintf('Sanity TX OK: %s\n', stripCR(all_TX(1)));

        % ---------- Fixed-rate replay ----------
        per = 1 / o.RateHz;
        for pass = 1:o.Loop
            fprintf('\nPass %d/%d...\n', pass, o.Loop);
            t0 = tic;
            for i=1:N_total
                sendLine(s, all_TX(i));
                % keep uniform spacing referenced to start time
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