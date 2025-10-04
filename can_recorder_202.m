function can_recorder_202(action, filename_base)
% CAN_RECORDER_202 — Minimal SLCAN logger that records ONLY ID_hex == "202"
%
% Usage:
%   can_recorder_202('start')              % start, auto filename
%   can_recorder_202('start','my_log')     % start with custom base name
%   can_recorder_202('status')             % basic status (msgs, rate)
%   can_recorder_202('stop')               % stop and save CSV + MAT
%
% CSV columns (exactly):
%   ID_hex, DLC, b0, b1, b2, b3, b4, b5, b6, b7
%
% MAT variables:
%   CAN_202  - detailed table: Timestamp, [SLCAN_TS_ms], [SLCAN_TS_unwrapped_ms],
%              DLC, DATA_hex, b0..b7, Raw
%   META     - capture metadata

    % ======= USER CONFIG =======
    PORT = "COM5";
    UART_BAUD = 921600;          % 921600, 1000000, 2000000, 3000000
    CAN_RATE = "S8";             % S0..S8 (S8 = 1 Mbps)
    DISABLE_TS = false;          % false -> Z1 (timestamps ON), true -> Z0 (OFF)
    INPUT_BUFFER_BYTES = 2^20;   % 1 MiB
    STATUS_INTERVAL_S = 2.0;     % console status cadence
    TARGET_ID = "202";           % only record this 11-bit ID (hex string, no 0x)
    % ============================

    persistent s running start_t fname idx capacity ...
               t_vec ts16_vec ts_unw_vec dlc_vec data_hex_vec bytes_mat raw_vec ...
               total_seen ts_seen last_ts epoch

    if nargin < 1, action = 'help'; end
    switch lower(action)
        case 'start'
            if ~isempty(running) && running
                fprintf('⚠ Already running. Use can_recorder_202(''stop'').\n');
                return;
            end

            if nargin < 2 || strlength(string(filename_base))==0
                tstamp = string(datetime("now"), "yyyy-MM-dd_HH-mm-ss");
                fname = "CAN202_" + tstamp;
            else
                fname = string(filename_base);
            end

            % preallocate (will grow if needed)
            capacity = 50000;
            idx = 0;
            t_vec         = zeros(1,capacity,'double');
            ts16_vec      = nan(1,capacity);
            ts_unw_vec    = nan(1,capacity);
            dlc_vec       = zeros(1,capacity,'uint8');
            data_hex_vec  = strings(1,capacity);
            bytes_mat     = zeros(capacity,8,'uint8');
            raw_vec       = strings(1,capacity);

            total_seen = 0; ts_seen = 0; last_ts = NaN; epoch = 0;

            % open serial + configure SLCAN
            try
                s = serialport(PORT, UART_BAUD);
                s.Timeout = 0.01;
                s.InputBufferSize = INPUT_BUFFER_BYTES;
                configureTerminator(s, "CR");
                flush(s);

                writeline(s,"C");            pause(0.02);
                writeline(s,CAN_RATE);       pause(0.02);
                writeline(s,"m00000000");    pause(0.02); % accept-all
                writeline(s,"M00000000");    pause(0.02);
                writeline(s, tern(DISABLE_TS,"Z0","Z1")); pause(0.02);
                writeline(s,"O");            pause(0.02);

                % optional version probe
                try
                    writeline(s,"V"); pause(0.02);
                    if s.NumBytesAvailable > 0, readline(s); end
                catch, end
                flush(s);
            catch ME
                fprintf('❌ Serial/SLCAN init failed: %s\n', ME.message);
                try, clear s; end
                return;
            end

            running = true;
            start_t = tic;
            fprintf('🔴 Recording ONLY ID_hex == %s on %s @ %d baud, CAN 1 Mbps, TS %s\n', ...
                TARGET_ID, PORT, UART_BAUD, tern(DISABLE_TS,'OFF','ON'));
            fprintf('Files will be: %s_202.csv / %s_202.mat\n', fname, fname);

            run_loop(); % blocking loop; returns when stopped

        case 'status'
            if isempty(running) || ~running
                fprintf('⚪ Not running.\n'); return;
            end
            elapsed = toc(start_t);
            rate = idx/max(elapsed,eps);
            ts_pct = 100*(ts_seen/max(total_seen,1));
            fprintf('🔴 RUNNING | t=%.1fs | kept=%d (%.0f/s) | TS seen=%.0f%%\n', elapsed, idx, rate, ts_pct);

        case 'stop'
            if isempty(running) || ~running
                fprintf('⚠ Not running.\n'); return;
            end
            running = false;
            try
                if exist('s','var') && ~isempty(s)
                    writeline(s,"C"); pause(0.02); clear s;
                end
            catch, end
            save_out();

        otherwise
            fprintf('can_recorder_202 commands:\n');
            fprintf('  can_recorder_202(''start''[, ''name''])\n');
            fprintf('  can_recorder_202(''status'')\n');
            fprintf('  can_recorder_202(''stop'')\n');
    end

    % ========= internal functions =========

    function run_loop()
        last_print = tic;
        partial = "";

        while running
            try
                nb = s.NumBytesAvailable;
                if nb > 0
                    chunk = read(s, nb, "string");
                    if ismissing(chunk), chunk = ""; end
                    partial = partial + chunk;

                    parts = split(partial, char(13)); % CR
                    nfull = numel(parts)-1;
                    if nfull > 0
                        lines = parts(1:nfull);
                        partial = parts(end);
                        tnow = toc(start_t);
                        for k = 1:nfull
                            line = strtrim(lines(k));
                            if strlength(line)==0, continue; end
                            h = extractBetween(line,1,1);

                            % only parse SLCAN data frames
                            if ~(h=="t" || h=="T"), continue; end
                            [id_hex, dlc, data_hex, bytes_row] = parse_basic(line);

                            % track TS presence stats
                            total_seen = total_seen + 1;

                            % filter strictly ID_hex == "202"
                            if id_hex ~= TARGET_ID, continue; end

                            % parse (optional) 16-bit TS and unwrap
                            ts16 = NaN;
                            if ~DISABLE_TS, ts16 = parse_ts(line, dlc, h); end
                            if ~isnan(ts16), ts_seen = ts_seen + 1; end
                            [ts_unw, epoch, last_ts] = unwrap_ts(ts16, epoch, last_ts);

                            add_row(tnow, ts16, ts_unw, dlc, data_hex, bytes_row, line);
                        end
                    end

                    if toc(last_print) >= STATUS_INTERVAL_S
                        if idx > 0
                            elapsed = toc(start_t);
                            fprintf('… kept=%d | t=%.1fs | ≈%.0f/s\n', idx, elapsed, idx/max(elapsed,eps));
                        else
                            fprintf('… no matching frames yet.\n');
                        end
                        last_print = tic;
                    end
                else
                    pause(0); % yield
                end
            catch ME
                fprintf('Loop error: %s\n', ME.message);
                break;
            end
        end

        % ensure stop/save even if loop breaks
        try
            if exist('s','var') && ~isempty(s)
                writeline(s,"C"); pause(0.02); clear s;
            end
        catch, end
        save_out();
    end

    function add_row(tnow, ts16, ts_unw, dlc, data_hex, bytes_row, line)
        if idx >= capacity
            % grow arrays
            newcap = capacity * 2;
            t_vec(newcap)        = 0;
            ts16_vec(newcap)     = NaN;
            ts_unw_vec(newcap)   = NaN;
            dlc_vec(newcap)      = 0;
            data_hex_vec(newcap) = "";
            bytes_mat(newcap,8)  = 0;
            raw_vec(newcap)      = "";
            capacity = newcap;
        end
        idx = idx + 1;
        t_vec(idx)        = tnow;
        ts16_vec(idx)     = ts16;
        ts_unw_vec(idx)   = ts_unw;
        dlc_vec(idx)      = uint8(max(0,dlc));
        data_hex_vec(idx) = data_hex;
        bytes_mat(idx,:)  = bytes_row;
        raw_vec(idx)      = string(line);
    end

    function save_out()
        N = idx;
        csv_file = char(fname + "_202.csv");
        mat_file = char(fname + "_202.mat");

        if N == 0
            fprintf('⚪ No frames with ID 0x%s captured. Nothing to save.\n', TARGET_ID);
            return;
        end

        % ==== CSV (only these columns) ====
        % ID_hex as the literal 3-nibble code "202" (no 0x), DLC, then b0..b7
        Tcsv = table( ...
            repmat(string(TARGET_ID), N, 1), ...   % ID_hex column "202"
            double(dlc_vec(1:N)).', ...            % DLC
            bytes_mat(1:N,1), ...                  % b0
            bytes_mat(1:N,2), ...                  % b1
            bytes_mat(1:N,3), ...                  % b2
            bytes_mat(1:N,4), ...                  % b3
            bytes_mat(1:N,5), ...                  % b4
            bytes_mat(1:N,6), ...                  % b5
            bytes_mat(1:N,7), ...                  % b6
            bytes_mat(1:N,8), ...                  % b7
            'VariableNames', {'ID_hex','DLC','b0','b1','b2','b3','b4','b5','b6','b7'} ...
        );
        try
            writetable(Tcsv, csv_file);
            fprintf('✓ CSV saved: %s  (rows=%d)\n', csv_file, N);
        catch ME
            fprintf('❌ CSV save error: %s\n', ME.message);
        end

        % ==== MAT (unchanged: detailed table for analysis) ====
        have_ts = any(~isnan(ts16_vec(1:N)));
        T = table( t_vec(1:N).', 'VariableNames', {'Timestamp'} );
        if have_ts
            T.SLCAN_TS_ms = ts16_vec(1:N).';
            T.SLCAN_TS_unwrapped_ms = ts_unw_vec(1:N).';
        end
        T.DLC = double(dlc_vec(1:N)).';
        T.DATA_hex = data_hex_vec(1:N).';
        for j=1:8, T.(sprintf('b%d',j-1)) = bytes_mat(1:N,j); end
        T.Raw = raw_vec(1:N).';

        META = struct();
        META.port = char(PORT);
        META.uart_baud = UART_BAUD;
        META.can_rate_cmd = char(CAN_RATE);
        META.timestamps_requested = ~DISABLE_TS;
        META.timestamps_present   = have_ts;
        META.start_datetime = datetime("now");
        META.elapsed_s = T.Timestamp(end) - T.Timestamp(1);
        META.kept_rows = N;
        META.target_id_hex = char(TARGET_ID);

        try
            CAN_202 = T; %#ok<NASGU>
            save(mat_file, 'CAN_202','META','-v7.3');
            fprintf('✓ MAT saved: %s  (vars: CAN_202, META)\n', mat_file);
        catch ME
            fprintf('❌ MAT save error: %s\n', ME.message);
        end
    end

    % ======= helpers =======
    function [id_hex, dlc, data_hex, bytes_row] = parse_basic(line)
        try
            if startsWith(line,"t")           % 11-bit
                id_hex = upper(extractBetween(line,2,4));
                dlc    = hex2dec(extractBetween(line,5,5));
                if dlc>0
                    data_hex = upper(extractBetween(line,6, 5+2*dlc));
                else
                    data_hex = "";
                end
            elseif startsWith(line,"T")       % 29-bit (won't match TARGET_ID)
                id_hex = upper(extractBetween(line,2,9));
                dlc    = hex2dec(extractBetween(line,10,10));
                if dlc>0
                    data_hex = upper(extractBetween(line,11, 10+2*dlc));
                else
                    data_hex = "";
                end
            else
                id_hex = ""; dlc = -1; data_hex = "";
            end

            bytes_row = zeros(1,8,'uint8');
            if dlc > 0
                dh = char(data_hex);
                for j = 1:min(dlc,8)
                    p = (j-1)*2 + 1;
                    if p+1 <= length(dh)
                        bytes_row(j) = uint8(hex2dec(dh(p:p+1)));
                    end
                end
            end
        catch
            id_hex = ""; dlc = -1; data_hex = ""; bytes_row = zeros(1,8,'uint8');
        end
    end

    function ts16 = parse_ts(line, dlc, firstChar)
        try
            if dlc < 0, ts16 = NaN; return; end
            if firstChar=="t"
                ts_start = 6 + 2*dlc;
            else
                ts_start = 11 + 2*dlc;
            end
            ts_end = ts_start + 3;
            if strlength(line) >= ts_end
                ts16 = hex2dec(extractBetween(line, ts_start, ts_end));
            else
                ts16 = NaN;
            end
        catch
            ts16 = NaN;
        end
    end

    function [ts_unw, epoch_out, last_out] = unwrap_ts(ts16, epoch_in, last_in)
        if isnan(ts16)
            ts_unw = NaN; epoch_out = epoch_in; last_out = last_in; return;
        end
        WRAP = 65536; % ms
        epoch_out = epoch_in; last_out = ts16;
        if ~isnan(last_in)
            if ts16 + 5000 < last_in   % 5 s hysteresis
                epoch_out = epoch_in + WRAP;
            end
        end
        ts_unw = double(ts16) + epoch_out;
    end

    function out = tern(cond,a,b), if cond, out=a; else, out=b; end, end
end
