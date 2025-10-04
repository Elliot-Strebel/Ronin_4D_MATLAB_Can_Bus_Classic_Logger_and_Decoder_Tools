function can_recorder(action, filename_or_label)
% CAN_RECORDER - SLCAN CAN logger with CSV & MAT export, LIVE analytics, and a control panel
%
% Usage:
%   can_recorder('start')                        % Start recording (default filename)
%   can_recorder('start','my_data')              % Start recording with custom filename (no extension)
%   can_recorder('note','TurningLeft')           % Set current label via code (panel preferred)
%   can_recorder('status')                       % Show status + live analytics
%   can_recorder('stop')                         % Stop & save CSV+MAT manually
%
% Control panel while running:
%   Buttons: Idle, TurningLeft, TurningRight, Custom..., Stop & Save
%   Hotkeys (panel focused): I, L, R, Space=MARK, Enter=Custom, ←/→ = Left/Right
%
% CSV columns (auto):
%   Timestamp | [SLCAN_TS_ms | SLCAN_TS_unwrapped_ms if present] | ID_hex | ID_dec | isExtended | DLC | DATA_hex | b0..b7 | Label | Raw_Message | Notes
%
% MAT variables:
%   CAN               - full table (as above, includes Label)
%   BYTES             - compact: ID_hex, ID_dec, isExtended, DLC, b0..b7(uint8), Timestamp,
%                        [SLCAN_TS_ms], [SLCAN_TS_unwrapped_ms], Label
%   BYTES_by_id       - struct of per-ID tables: Timestamp, DLC, b0..b7,
%                        [SLCAN_TS_ms], [SLCAN_TS_unwrapped_ms], Label
%   DATA_bytes_uint8  - N×8 uint8 payloads
%   EVENTS_saved      - struct array of label events {name, t, idx}
%   META              - capture metadata

    % ==== USER TUNABLES ====================================================
    PORT = "COM5";
    UART_BAUD = 921600;            % 921600, 1000000, 2000000, 3000000 (if supported)
    CAN_RATE = "S8";               % S0..S8 = 10k..1M (S8 = 1 Mbps)
    DISABLE_TS = false;            % timestamps ON by default (Z1); true = Z0 (OFF)
    INPUT_BUFFER_BYTES = 2^20;     % 1 MiB serial input buffer
    STATUS_INTERVAL_S = 2.0;       % print status + live analytics cadence
    STUFF_FACTOR = 1.15;           % ~15% bit stuffing
    TOP_N = 5;                     % show top talkers

    % --- LIVE ANALYTICS WINDOW ---
    LIVE_WINDOW_S = 6.0;
    MIN_FRAMES_PER_ID = 8;
    JITTER_GOOD = 12;

    % --- LIVE VIEW (IDs shown in panel) ---
    CMD_ID = "202";        % command stream (stick)
    HB_ID  = "604";        % heartbeat/status ID for counter
    % =======================================================================

    persistent recording_active s start_time filename_base ...
               raw_lines timestamps slcan_ts_raw slcan_ts_unwrapped ...
               id_hex_list id_dec_list is_ext_list dlc_list data_hex_list DATA_bytes ...
               msg_count idx capacity bits_accum win_count win_start id_counts ...
               ts_epoch last_ts EVENTS last_note_t last_note_label ...
               ctrlFig pending_notes ui_stop_requested ...
               current_label labels_list ts_seen total_seen

    % ---- UI handles (captured by nested functions) ----
    hStick = []; hHB = []; hTable = []; hStatus = []; last_hb_val = NaN;

    if nargin < 1
        action = 'help';
    end

    switch lower(action)
        case 'start'
            if isempty(recording_active) || ~recording_active
                % Filename base
                if nargin >= 2
                    filename_base = filename_or_label;
                else
                    tstamp = string(datetime("now"), "yyyy-MM-dd_HH-mm-ss");
                    filename_base = "CAN_recording_" + tstamp;
                end

                % Preallocate (auto-grows)
                capacity   = 100000;
                raw_lines  = cell(1, capacity);
                timestamps = zeros(1, capacity, 'double');
                slcan_ts_raw = nan(1, capacity);          % 16-bit ms (0..65535), NaN if absent
                slcan_ts_unwrapped = nan(1, capacity);     % unwrapped ms (monotonic), NaN if absent
                id_hex_list = cell(1, capacity);
                id_dec_list = zeros(1, capacity, 'uint32');
                is_ext_list = false(1, capacity);
                dlc_list    = zeros(1, capacity, 'uint8');
                data_hex_list = cell(1, capacity);
                DATA_bytes  = zeros(capacity, 8, 'uint8');
                labels_list = strings(1, capacity);   % per-frame label

                idx = 0; msg_count = 0; bits_accum = 0;
                win_count = 0; win_start = tic;
                id_counts = containers.Map('KeyType','char','ValueType','double');
                ts_epoch = 0; last_ts = NaN;
                EVENTS = struct('name',{},'t',{},'idx',{});
                last_note_t = 0; last_note_label = "START";
                current_label = "Idle";               % default label applied immediately
                recording_active = true;

                % timestamp presence counters
                ts_seen = 0; total_seen = 0;

                % init live-view state
                last_hb_val = NaN;

                fprintf('🔴 Starting CAN recording…\n');
                fprintf('Files will be: %s.csv and %s.mat\n', filename_base, filename_base);
                fprintf('Port: %s | UART: %d baud | CAN: 1 Mbps | TS:%s (requested)\n\n', ...
                        PORT, UART_BAUD, tern(DISABLE_TS,'OFF','ON'));

                % Serial open & SLCAN config
                try
                    s = serialport(PORT, UART_BAUD);
                    s.Timeout = 0.01;
                    s.InputBufferSize = INPUT_BUFFER_BYTES;
                    configureTerminator(s, "CR");
                    flush(s);

                    writeline(s, "C");              pause(0.02);
                    writeline(s, CAN_RATE);         pause(0.02);
                    writeline(s, "m00000000");      pause(0.02); % accept-all
                    writeline(s, "M00000000");      pause(0.02);
                    if DISABLE_TS
                        writeline(s, "Z0");
                    else
                        writeline(s, "Z1");
                    end
                    pause(0.02);
                    writeline(s, "O");              pause(0.02);

                    % Optional version probe, then flush
                    try
                        writeline(s, "V");
                        pause(0.02);
                        if s.NumBytesAvailable > 0
                            readline(s); % discard one line
                        end
                    catch MEv
                        fprintf('Version probe skipped: %s\n', MEv.message);
                    end
                    flush(s);

                    fprintf('✓ Connected to CANable on %s\n', PORT);
                    fprintf('✓ SLCAN open @ 1 Mbps, UART %d baud, timestamps %s (requested)\n\n', ...
                            UART_BAUD, tern(DISABLE_TS,'OFF','ON'));

                    % Control panel + start loop
                    pending_notes = strings(0,1);
                    ui_stop_requested = false;
                    create_control_panel();         % creates live-view UI too
                    start_time = tic;
                    % Log initial "Idle" event at start
                    set_current_label("Idle");
                    start_background_recording();

                catch ME
                    fprintf('❌ Error connecting: %s\n', ME.message);
                    recording_active = false;
                    try, clear s; catch, end
                    return;
                end
            else
                fprintf('⚠ Recording already active. Use can_recorder(''stop'')\n');
            end

        case 'note'
            if ~isempty(recording_active) && recording_active
                if nargin >= 2, lbl = string(filename_or_label); else, lbl = "MARK"; end
                set_current_label(lbl);
            else
                fprintf('⚪ Not recording. Start first to add labels.\n');
            end

        case 'stop'
            if ~isempty(recording_active) && recording_active
                recording_active = false;
                fprintf('\n🛑 Stopping recording…\n');
                try
                    if exist('s','var') && ~isempty(s)
                        writeline(s, "C"); pause(0.02); clear s;
                    end
                catch ME
                    fprintf('Stop/close error: %s\n', ME.message);
                end
                save_to_csv_and_mat();
                fprintf('✓ Recording stopped and saved.\n');
            else
                fprintf('⚠ No active recording.\n');
            end

        case 'status'
            if ~isempty(recording_active) && recording_active
                print_status_and_live();
            else
                fprintf('⚪ Not recording.\n');
            end

        otherwise
            fprintf('CAN Message Recorder (SLCAN) — commands:\n');
            fprintf('  can_recorder(''start'')               Start recording\n');
            fprintf('  can_recorder(''start'',''name'')       Start with custom filename\n');
            fprintf('  can_recorder(''note'',''Label'')       Set current label (or use control panel)\n');
            fprintf('  can_recorder(''status'')              Show status + live analytics\n');
            fprintf('  can_recorder(''stop'')                Stop & save CSV+MAT\n');
    end

    %=========== background loop & live analytics ===========

    function start_background_recording()
        last_print = tic;
        partial = "";  % empty string (NOT <missing>)

        while recording_active
            try
                nb = s.NumBytesAvailable;
                if nb > 0
                    chunk = read(s, nb, "string");
                    if ismissing(chunk), chunk = ""; end
                    partial = partial + chunk;

                    % Split on CR; keep the remainder
                    parts = split(partial, char(13));  % "\r"
                    nfull = numel(parts) - 1;
                    if nfull > 0
                        lines = parts(1:nfull);
                        partial = parts(end);

                        tnow = toc(start_time);
                        for k2 = 1:nfull
                            line = strtrim(lines(k2));
                            if ismissing(line) || strlength(line) == 0, continue; end

                            h = extractBetween(line,1,1);
                            if h=="t" || h=="T"
                                [id_hex, id_dec, is_ext, dlc, data_hex, bytes_row] = parse_full(line);

                                ts16 = NaN;
                                if ~DISABLE_TS
                                    ts16 = parse_ts(line, dlc, h);
                                end
                                [ts_unwrapped, ts_epoch, last_ts] = unwrap_ts(ts16, ts_epoch, last_ts);

                                add_msg(line, tnow, ts16, ts_unwrapped, id_hex, id_dec, is_ext, dlc, data_hex, bytes_row);

                                % track TS detection
                                total_seen = total_seen + 1;
                                if ~isnan(ts16), ts_seen = ts_seen + 1; end

                                if dlc >= 0
                                    bits_no_stuff = 47 + 8*double(dlc);
                                    bits_accum = bits_accum + round(bits_no_stuff * STUFF_FACTOR);
                                end

                                if ~isempty(id_hex)
                                    key = safe_char(id_hex);
                                    if ~isKey(id_counts, key), id_counts(key) = 0; end
                                    id_counts(key) = id_counts(key) + 1;
                                end
                            end
                        end
                    end

                    % Status + live analytics heartbeat
                    if toc(last_print) >= STATUS_INTERVAL_S
                        print_status_and_live();
                        try
                            update_live_view_ui(toc(start_time));
                        catch MEu
                            fprintf('Live-view UI update error: %s\n', MEu.message);
                        end
                        last_print = tic;
                    end
                else
                    pause(0); % tiny yield
                end

                % Process UI events & pending label changes
                flush_pending_notes();
                drawnow limitrate;
                if ui_stop_requested, recording_active = false; end

            catch ME
                fprintf('Recording error: %s\n', ME.message);
                break;
            end
        end

        % If UI requested stop: close & save here
        if ui_stop_requested
            try
                if exist('s','var') && ~isempty(s)
                    writeline(s, "C"); pause(0.02); clear s;
                end
            catch ME
                fprintf('UI stop close error: %s\n', ME.message);
            end
            save_to_csv_and_mat();
            fprintf('✓ Recording stopped and saved (UI Stop).\n');
        end
    end

    function print_status_and_live()
        elapsed = toc(start_time);
        [ceil_no_ts, ceil_ts] = serial_ceiling(UART_BAUD);
        rate = msg_count/max(elapsed,eps);
        busload = (bits_accum / max(elapsed,eps)) / 1e6;
        win_elapsed = toc(win_start);
        win_rate = win_count / max(win_elapsed,eps);
        active_ceil = tern_num(DISABLE_TS, ceil_no_ts, ceil_ts);
        alt_ceil    = tern_num(DISABLE_TS, ceil_ts, ceil_no_ts);

        ts_pct = 100 * (ts_seen / max(total_seen,1));
        ts_hint = sprintf('TS seen: %.0f%%', ts_pct);

        fprintf('\n🔴 RECORDING ACTIVE | t=%.1fs | msgs=%d | avg %.0f/s | last-1s %.0f/s | ~%.1f%% bus load | Label="%s" | %s\n', ...
                elapsed, msg_count, rate, win_rate, 100*busload, char(current_label), ts_hint);
        fprintf('Serial ceiling (active): ~%.0f msg/s | alternative: ~%.0f msg/s\n', active_ceil, alt_ceil);
        print_top_talkers(id_counts, elapsed, TOP_N);

        if win_elapsed >= 1.0
            win_count = 0;
            win_start = tic;
        end

        % Live analytics over rolling window (console)
        try
            tnow = elapsed; t0 = max(0, tnow - LIVE_WINDOW_S);
            if idx > 0, live_patterns(t0, tnow); end
        catch ME
            fprintf('Live analysis error: %s\n', ME.message);
        end
    end

    function live_patterns(t0, t1)
        N = idx; if N==0, return; end
        tvec = timestamps(1:N);
        inwin = (tvec >= t0) & (tvec <= t1);
        if ~any(inwin), fprintf('LIVE: no frames in the last %.1fs window.\n', t1 - t0); return; end

        ids  = string(id_hex_list(1:N));
        dlcs = double(dlc_list(1:N));
        bytes = DATA_bytes(1:N,:);
        labs = string(labels_list(1:N));

        idsW = ids(inwin); tW = tvec(inwin); dlcW = dlcs(inwin);
        bW   = bytes(inwin,:); labW = labs(inwin);

        [uids, ~, g] = unique(idsW);
        nIDs = numel(uids);
        dur = max(tW) - min(tW); if dur<=0, dur = t1 - t0; end

        LIVE = table('Size',[nIDs 8], ...
            'VariableTypes',{'string','double','double','double','double','double','double','string'}, ...
            'VariableNames',{'ID_hex','Count','Rate_Hz','MedianP_ms','Std_ms','Jitter_pct','Score','DominantLabel'});

        byteHints = strings(nIDs,1);
        cntHints  = strings(nIDs,1);

        for i = 1:nIDs
            sel = (g == i);
            tk = sort(tW(sel));
            n  = numel(tk);
            LIVE.ID_hex(i) = uids(i);
            LIVE.Count(i)  = n;
            LIVE.Rate_Hz(i)= n/max(dur,eps);

            lab_i = labW(sel);
            if ~isempty(lab_i)
                [ul,~,gl] = unique(lab_i);
                cts = accumarray(gl,1);
                [~,ii] = max(cts);
                LIVE.DominantLabel(i) = ul(ii);
            else
                LIVE.DominantLabel(i) = "";
            end

            if n >= MIN_FRAMES_PER_ID
                di_ms = 1000*diff(tk);
                LIVE.MedianP_ms(i) = median(di_ms);
                LIVE.Std_ms(i)     = std(di_ms);
                LIVE.Jitter_pct(i) = 100*LIVE.Std_ms(i)/max(LIVE.MedianP_ms(i),eps);
                LIVE.Score(i)      = max(0, 100 - LIVE.Jitter_pct(i)) * min(1,n/50);

                rows = find(inwin); rows = rows(sel);
                dlc_i = dlcW(rows); b_i = bW(rows,:);
                changes = false(1,8);
                for j = 1:8
                    present = dlc_i >= j;
                    if any(present)
                        bj = b_i(present,j);
                        changes(j) = (numel(unique(bj)) > 1);
                    end
                end
                byteHints(i) = "chg bytes: " + join(string(find(changes)-1),',');

                % counter hint on mod256 diffs
                bestScore = 0; bestByte = -1; bestMod = 0;
                for j = 1:8
                    present = dlc_i >= j;
                    if nnz(present) >= 5
                        bj = double(b_i(present,j));
                        dx = mod(diff(bj), 256);
                        sc = mean((dx==1) | (dx==0));
                        if sc > bestScore
                            bestScore = sc; bestByte = j-1; bestMod = 256;
                        end
                    end
                end
                if bestScore >= 0.6
                    cntHints(i) = sprintf('counter b%d mod%d (%.2f)', bestByte, bestMod, bestScore);
                else
                    cntHints(i) = "";
                end
            else
                LIVE.MedianP_ms(i) = NaN;
                LIVE.Std_ms(i)     = NaN;
                LIVE.Jitter_pct(i) = NaN;
                LIVE.Score(i)      = 0;
                byteHints(i)       = "";
                cntHints(i)        = "";
            end
        end

        LIVE = sortrows(LIVE, {'Score','Rate_Hz','Count'},{'descend','descend','descend'});

        seg = sprintf('segment: "%s", window %.1fs', char(last_note_label), t1 - t0);
        fprintf('LIVE (%s): IDs=%d\n', seg, nIDs);

        topP = min(6, height(LIVE));
        for i = 1:topP
            idh = LIVE.ID_hex(i); n = LIVE.Count(i);
            r   = LIVE.Rate_Hz(i); mp = LIVE.MedianP_ms(i);
            jit = LIVE.Jitter_pct(i); sc = LIVE.Score(i);
            fprintf('   ◦ ID %s | N=%d | %.2f Hz | P≈%.1f ms | jitter %.1f%% | score %.0f | label:%s\n', ...
                idh, n, r, mp, jit, sc, LIVE.DominantLabel(i));
        end

        LIVE2 = sortrows(LIVE, {'Rate_Hz','Count'},{'descend','descend'});
        for i = 1:min(3,height(LIVE2))
            idh = LIVE2.ID_hex(i);
            im  = find(LIVE.ID_hex==idh,1);
            if im>0
                if strlength(byteHints(im))>0 || strlength(cntHints(im))>0
                    fprintf('     ↳ %s | %s\n', byteHints(im), cntHints(im));
                end
            end
        end
    end

    % ---- Embedded live-view updater ----
    function update_live_view_ui(tnow)
        if ~(exist('ctrlFig','var') && ishghandle(ctrlFig)), return; end
        if any([isempty(hStick), isempty(hHB), isempty(hTable), isempty(hStatus)]), return; end
        try
            N = idx; if N <= 0, set(hStatus,'String','No data yet…'); return; end

            t = timestamps(1:N);
            t0 = max(0, tnow - LIVE_WINDOW_S);
            inW = (t >= t0) & (t <= tnow);
            if ~any(inW)
                set(hStatus,'String',sprintf('t=%.2fs | window=%.1fs | rows=0', tnow, LIVE_WINDOW_S)); return;
            end

            % Stick from CMD_ID (int16LE b2:b3)
            ids = string(id_hex_list(1:N));
            dataW = DATA_bytes(1:N,:);
            cmdSel = inW & (ids==CMD_ID);
            if any(cmdSel)
                b2 = uint16(dataW(cmdSel,3)); b3 = uint16(dataW(cmdSel,4));
                u16 = b2 + bitshift(b3,8);
                i16 = typecast(u16,'int16');
                stickStr = sprintf('%d', i16(end));
            else
                stickStr = '--';
            end
            set(hStick,'String',stickStr);

            % Heartbeat from HB_ID b6 (value / Δ)
            hbSel = inW & (ids==HB_ID);
            if any(hbSel)
                b6v = double(dataW(hbSel,7));
                last = b6v(end);
                if isnan(last_hb_val), d = NaN; else, d = mod(last - last_hb_val, 256); end
                last_hb_val = last;
                if isnan(d), hbStr = sprintf('%.0f / Δ=--', last);
                else,        hbStr = sprintf('%.0f / Δ=%.0f', last, d);
                end
            else
                hbStr = '--';
            end
            set(hHB,'String',hbStr);

            % Per-ID rates in window
            idsW = ids(inW);
            if ~isempty(idsW)
                [u,~,g] = unique(idsW);
                counts = accumarray(g,1);
                tw = t(inW);
                dur = max(tw) - min(tw); if dur <= 0, dur = LIVE_WINDOW_S; end
                rates = counts / max(dur, eps);
                [rates,ord] = sort(rates,'descend'); u = u(ord); counts = counts(ord);
                top = min(6, numel(u));
                data = cell(top,3);
                for i=1:top
                    data{i,1} = char(u(i)); data{i,2} = rates(i); data{i,3} = counts(i);
                end
                set(hTable,'Data',data);
            end

            set(hStatus,'String',sprintf('t=%.2fs | window=%.1fs | rows=%d', tnow, LIVE_WINDOW_S, nnz(inW)));
        catch ME
            set(hStatus,'String',sprintf('Live-view error: %s', ME.message));
        end
    end

    function add_msg(line, tnow, ts16, ts_unwrapped, id_hex, id_dec, is_ext, dlc, data_hex, bytes_row)
        if idx >= capacity
            capacity = capacity * 2;
            raw_lines{capacity} = [];
            timestamps(capacity) = 0;
            slcan_ts_raw(capacity) = NaN;
            slcan_ts_unwrapped(capacity) = NaN;
            id_hex_list{capacity} = [];
            id_dec_list(capacity) = 0;
            is_ext_list(capacity) = false;
            dlc_list(capacity) = 0;
            data_hex_list{capacity} = [];
            DATA_bytes(capacity,8) = 0;
            labels_list(capacity) = "";
        end
        idx = idx + 1;
        raw_lines{idx} = safe_char(line);
        timestamps(idx) = tnow;
        slcan_ts_raw(idx) = ts16;
        slcan_ts_unwrapped(idx) = ts_unwrapped;

        id_hex_list{idx} = safe_char(id_hex);
        id_dec_list(idx) = uint32(id_dec);
        is_ext_list(idx) = logical(is_ext);
        dlc_list(idx) = uint8(max(0,dlc));
        data_hex_list{idx} = safe_char(data_hex);
        DATA_bytes(idx,:) = bytes_row;

        labels_list(idx) = string(current_label);

        msg_count = msg_count + 1;
        win_count = win_count + 1;
    end

    % ---- Parsing helpers ----
    function [id_hex, id_dec, is_ext, dlc, data_hex, bytes_row] = parse_full(line)
        try
            if startsWith(line,"t")           % 11-bit
                id_hex = upper(extractBetween(line,2,4));
                id_dec = hex2dec(id_hex);
                is_ext = false;
                dlc    = hex2dec(extractBetween(line,5,5));
                nibbles = 2*dlc;
                if nibbles > 0
                    data_hex = upper(extractBetween(line,6, 5+nibbles));
                else
                    data_hex = "";
                end
            elseif startsWith(line,"T")       % 29-bit
                id_hex = upper(extractBetween(line,2,9));
                id_dec = hex2dec(id_hex);
                is_ext = true;
                dlc    = hex2dec(extractBetween(line,10,10));
                nibbles = 2*dlc;
                if nibbles > 0
                    data_hex = upper(extractBetween(line,11, 10+nibbles));
                else
                    data_hex = "";
                end
            else
                id_hex = ""; id_dec = NaN; is_ext = false; dlc = -1; data_hex = "";
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
        catch ME
            fprintf('parse_full error: %s\n', ME.message);
            id_hex = ""; id_dec = NaN; is_ext = false; dlc = -1; data_hex = "";
            bytes_row = zeros(1,8,'uint8');
        end
    end

    function ts16 = parse_ts(line, dlc, firstChar)
        try
            if isnan(dlc) || dlc < 0
                ts16 = NaN; return;
            end
            if firstChar=="t"
                ts_start = 6 + 2*dlc;
            else
                ts_start = 11 + 2*dlc;
            end
            ts_end = ts_start + 3;
            if strlength(line) >= ts_end
                ts_str = extractBetween(line, ts_start, ts_end);
                ts16 = hex2dec(ts_str);
            else
                ts16 = NaN;
            end
        catch ME
            fprintf('parse_ts error: %s\n', ME.message);
            ts16 = NaN;
        end
    end

    function [ts_unwrapped, epoch_out, last_out] = unwrap_ts(ts16, epoch_in, last_in)
        if isnan(ts16)
            ts_unwrapped = NaN;
            epoch_out = epoch_in; last_out = last_in; return;
        end
        WRAP = 65536; % ms
        epoch_out = epoch_in; last_out = last_in;
        if ~isnan(last_in)
            if ts16 + 5000 < last_in   % 5 s hysteresis for backward jump
                epoch_out = epoch_in + WRAP;
            end
        end
        ts_unwrapped = double(ts16) + epoch_out;
        last_out = ts16;
    end

    function print_top_talkers(mapCounts, elapsed, N)
        if isempty(mapCounts) || elapsed <= 0, return; end
        keysArr = mapCounts.keys;
        if isempty(keysArr), return; end
        valsArr = zeros(numel(keysArr),1);
        for ii = 1:numel(keysArr), valsArr(ii) = mapCounts(keysArr{ii}); end
        [valsSorted, ord] = maxk(valsArr, min(N,numel(valsArr)));
        for ii = 1:numel(ord)
            idk = keysArr{ord(ii)};
            fprintf('   #%d  ID %s  ~%.0f/s\n', ii, idk, valsSorted(ii)/elapsed);
        end
    end

% ---- Save CSV & MAT (auto-hide TS cols if none), plus BYTES tables & EVENTS ----
function save_to_csv_and_mat()
    if idx == 0
        fprintf('⚠ No messages to save.\n'); return;
    end
    fprintf('💾 Saving %d messages to CSV and MAT…\n', idx);

    csv_filename    = char(filename_base + ".csv");
    mat_filename    = char(filename_base + ".mat");
    events_filename = char(filename_base + "_events.csv");

    N = idx;
    bytes = DATA_bytes(1:N,:);               % N×8 uint8
    labs  = string(labels_list(1:N)).';      % N×1 string
    have_ts = any(~isnan(slcan_ts_raw(1:N)));

    % ---- Build per-row Notes + LabelChange (only on change rows) ----
    Notes = strings(N,1);
    LabelChange = false(N,1);
    if N >= 1
        LabelChange(1) = true; Notes(1) = "Start Label=" + labs(1);
        if N > 1
            chg = labs(2:N) ~= labs(1:N-1);
            LabelChange(2:N) = chg;
            Notes(2:N) = "Label=" + labs(2:N);
            Notes(~LabelChange) = "";
        end
    end

    % ---------- Build CAN (full) ----------
    T = table( ...
        timestamps(1:N).', ...
        string(id_hex_list(1:N)).', ...
        double(id_dec_list(1:N)).', ...
        logical(is_ext_list(1:N)).', ...
        double(dlc_list(1:N)).', ...
        string(data_hex_list(1:N)).', ...
        'VariableNames', {'Timestamp','ID_hex','ID_dec','isExtended','DLC','DATA_hex'} ...
    );
    if have_ts
        T = addvars(T, ...
            slcan_ts_raw(1:N).', slcan_ts_unwrapped(1:N).', ...
            'Before','ID_hex', ...
            'NewVariableNames', {'SLCAN_TS_ms','SLCAN_TS_unwrapped_ms'});
    end
    for j = 1:8, T.(sprintf('b%d',j-1)) = bytes(:,j); end
    T.Label       = labs;
    T.LabelChange = LabelChange;
    T.Raw_Message = string(raw_lines(1:N)).';
    T.Notes       = Notes;

    % ---------- Build BYTES (compact) ----------
    BYTES = table( ...
        string(id_hex_list(1:N)).', ...
        double(id_dec_list(1:N)).', ...
        logical(is_ext_list(1:N)).', ...
        uint8(dlc_list(1:N)).', ...
        'VariableNames', {'ID_hex','ID_dec','isExtended','DLC'} ...
    );
    for j = 1:8, BYTES.(sprintf('b%d',j-1)) = bytes(:,j); end
    BYTES.Timestamp = timestamps(1:N).';
    if have_ts
        BYTES.SLCAN_TS_ms            = slcan_ts_raw(1:N).';
        BYTES.SLCAN_TS_unwrapped_ms  = slcan_ts_unwrapped(1:N).';
    end
    BYTES.Label = labs;

    % ---------- Build BYTES_by_id (struct of per-ID tables) ----------
    uids = unique(string(id_hex_list(1:N)).');
    BYTES_by_id = struct();
    for ui = 1:numel(uids)
        idh = uids(ui);
        rows = (string(id_hex_list(1:N)).' == idh);
        Ti = table( ...
            timestamps(rows).', ...
            uint8(dlc_list(rows)).', ...
            'VariableNames', {'Timestamp','DLC'} ...
        );
        for j = 1:8, Ti.(sprintf('b%d',j-1)) = bytes(rows,j); end
        if have_ts
            Ti.SLCAN_TS_ms           = slcan_ts_raw(rows).';
            Ti.SLCAN_TS_unwrapped_ms = slcan_ts_unwrapped(rows).';
        end
        Ti.Label = labs(rows);
        fname = matlab.lang.makeValidName("ID_" + idh);
        BYTES_by_id.(fname) = Ti;
    end

    % ---------- Write CSV ----------
    try
        writetable(T, csv_filename);
        dur = timestamps(N);
        busload_pct = 100*((bits_accum/max(dur,eps))/1e6);
        fprintf('✓ CSV: %s\n', csv_filename);
        fprintf('📈 Total: %d | ⏱ Duration: %.2f s | 📊 Avg: %.1f msg/s | ~%.1f%% bus load\n', ...
                N, dur, N/max(dur,eps), busload_pct);
        if ~have_ts
            fprintf('ℹ No SLCAN timestamps detected in this run (columns omitted).\n');
        end
    catch ME
        fprintf('❌ Error saving CSV: %s\n', ME.message);
    end

    % ---------- Write EVENTS CSV ----------
    try
        if exist('EVENTS','var') && ~isempty(EVENTS)
            EvN = numel(EVENTS);
            t  = zeros(EvN,1); ix = zeros(EvN,1); nm = strings(EvN,1);
            for i=1:EvN, t(i) = EVENTS(i).t; ix(i) = EVENTS(i).idx; nm(i)= string(EVENTS(i).name); end
            EVENTS_T = table(t, ix, nm, 'VariableNames', {'t','idx','label'});
            writetable(EVENTS_T, events_filename);
            fprintf('✓ Events CSV: %s\n', events_filename);
        end
    catch ME
        fprintf('❌ Error saving events CSV: %s\n', ME.message);
    end

    % ---------- Save MAT ----------
    META = struct();
    META.port = char(PORT);
    META.uart_baud = UART_BAUD;
    META.can_rate_cmd = char(CAN_RATE);
    META.timestamps_requested = ~DISABLE_TS;
    META.timestamps_present   = have_ts;
    META.input_buffer_bytes = INPUT_BUFFER_BYTES;
    META.status_interval_s = STATUS_INTERVAL_S;
    META.stuff_factor = STUFF_FACTOR;
    META.top_n = TOP_N;
    META.live_window_s = LIVE_WINDOW_S;
    META.min_frames_per_id = MIN_FRAMES_PER_ID;
    META.jitter_good_pct = JITTER_GOOD;
    META.start_datetime = datetime("now");
    META.elapsed_s = timestamps(N);
    META.total_messages = N;
    META.estimated_busload_pct = 100*((bits_accum/max(META.elapsed_s,eps))/1e6);
    META.slcan_ts_seen_pct = 100*(ts_seen/max(total_seen,1));

    try
        CAN = T; %#ok<NASGU>
        DATA_bytes_uint8 = bytes; %#ok<NASGU>
        EVENTS_saved = EVENTS; %#ok<NASGU>
        save(mat_filename, 'CAN', 'BYTES', 'BYTES_by_id', 'EVENTS_saved', 'META', 'DATA_bytes_uint8', '-v7.3');
        fprintf('✓ MAT: %s  (vars: CAN, BYTES, BYTES_by_id, EVENTS_saved, META, DATA_bytes_uint8)\n', mat_filename);
    catch ME
        fprintf('❌ Error saving MAT: %s\n', ME.message);
    end
end


    %=========== control panel (UI) ===========

    function create_control_panel()
        try
            if exist('ctrlFig','var') && ishghandle(ctrlFig), delete(ctrlFig); end
        catch ME
            fprintf('UI cleanup warning: %s\n', ME.message);
        end

        % Bigger window to host live view
        ctrlFig = figure('Name','CAN Recorder Controls + Live View', 'NumberTitle','off', ...
            'MenuBar','none','ToolBar','none','Resize','off', ...
            'Position',[100 100 620 420], 'Color','w', ...
            'KeyPressFcn',@onKey, 'CloseRequestFcn',@onClose);

        % --- Label buttons row
        uicontrol(ctrlFig,'Style','pushbutton','String','Note: Idle', ...
            'Position',[20 370 110 30],'Callback',@(s,e)enqueue_note("Idle"));
        uicontrol(ctrlFig,'Style','pushbutton','String','Note: TurningLeft', ...
            'Position',[140 370 150 30],'Callback',@(s,e)enqueue_note("TurningLeft"));
        uicontrol(ctrlFig,'Style','pushbutton','String','Note: TurningRight', ...
            'Position',[300 370 150 30],'Callback',@(s,e)enqueue_note("TurningRight"));
        uicontrol(ctrlFig,'Style','pushbutton','String','Stop & Save', ...
            'Position',[460 370 140 30],'BackgroundColor',[1 .3 .3], ...
            'FontWeight','bold', 'Callback',@onStop);

        % --- Custom label
        uicontrol(ctrlFig,'Style','text','String','Custom label:', ...
            'Position',[20 335 100 22],'BackgroundColor','w','HorizontalAlignment','left');
        uicontrol(ctrlFig,'Style','edit','Tag','editNote','String','MARK', ...
            'Position',[120 335 220 24],'BackgroundColor','white');
        uicontrol(ctrlFig,'Style','pushbutton','String','Add', ...
            'Position',[350 335 60 24],'Callback',@onCustom);

        % --- Live View fields (new)
        uicontrol(ctrlFig,'Style','text','String',sprintf('0x%s stick (raw int16 b2:b3):', char(CMD_ID)), ...
            'Position',[20 300 250 22],'BackgroundColor','w','HorizontalAlignment','left','FontWeight','bold');
        hStick = uicontrol(ctrlFig,'Style','text','String','--', ...
            'Position',[280 300 300 22],'BackgroundColor','w','HorizontalAlignment','left');

        uicontrol(ctrlFig,'Style','text','String',sprintf('0x%s heartbeat b6 (last / Δ):', char(HB_ID)), ...
            'Position',[20 275 250 22],'BackgroundColor','w','HorizontalAlignment','left','FontWeight','bold');
        hHB = uicontrol(ctrlFig,'Style','text','String','--', ...
            'Position',[280 275 300 22],'BackgroundColor','w','HorizontalAlignment','left');

        uicontrol(ctrlFig,'Style','text','String','Window per-ID rates (Hz):', ...
            'Position',[20 248 250 22],'BackgroundColor','w','HorizontalAlignment','left','FontWeight','bold');

        hTable = uitable(ctrlFig,'Data',cell(0,3), ...
            'ColumnName',{'ID_hex','Rate_Hz','Count'}, 'ColumnEditable',[false false false], ...
            'Position',[20 70 580 170]);

        hStatus = uicontrol(ctrlFig,'Style','text','String','Initializing...', ...
            'Position',[20 20 580 30],'BackgroundColor','w','HorizontalAlignment','left');
    end

    function onCustom(~,~), addCustom(); end
    function addCustom()
        try
            h = findobj(ctrlFig,'Tag','editNote');
            if ~isempty(h), enqueue_note(string(h.String)); end
        catch ME
            fprintf('Custom label error: %s\n', ME.message);
        end
    end

    function onStop(~,~)
        ui_stop_requested = true;
        fprintf('🛑 UI Stop requested — finishing current cycle then saving…\n');
    end

    function onClose(~,~)
        try
            if ishghandle(ctrlFig), set(ctrlFig,'Visible','off'); end
        catch ME
            fprintf('UI close warning: %s\n', ME.message);
        end
    end

    function onKey(~,evt)
        key = lower(string(evt.Key));
        if     key=="i",         enqueue_note("Idle");
        elseif key=="l",         enqueue_note("TurningLeft");
        elseif key=="r",         enqueue_note("TurningRight");
        elseif key=="space",     enqueue_note("MARK");
        elseif key=="return" || key=="enter", addCustom();
        elseif key=="leftarrow",  enqueue_note("TurningLeft");
        elseif key=="rightarrow", enqueue_note("TurningRight");
        end
    end

    function enqueue_note(lbl)
        try, pending_notes(end+1,1) = lbl;
        catch ME, fprintf('enqueue_note error: %s\n', ME.message);
        end
    end

    function set_current_label(lbl)
        try
            tnow = toc(start_time);
            current_label = string(lbl);
            EVENTS(end+1) = struct('name', current_label, 't', tnow, 'idx', idx+1);
            last_note_t = tnow; last_note_label = current_label;
            fprintf('🏷️  Label set → "%s" at t=%.3f s (applies to subsequent frames)\n', char(current_label), tnow);
        catch ME
            fprintf('set_current_label error: %s\n', ME.message);
        end
    end

    function flush_pending_notes()
        try
            while ~isempty(pending_notes)
                lbl = pending_notes(1); pending_notes(1) = [];
                set_current_label(lbl);
            end
        catch ME
            fprintf('flush_pending_notes error: %s\n', ME.message);
        end
    end

    %=========== misc helpers ===========

    function out = tern(cond, a, b), if cond, out = a; else, out = b; end, end
    function out = tern_num(cond, a, b), if cond, out = a; else, out = b; end, end

    function c = safe_char(s)
        if isstring(s), if ismissing(s), c = ''; else, c = char(s); end
        elseif ischar(s), c = s;
        else, c = char(string(s));
        end
    end

    function [ceil_no_ts, ceil_ts] = serial_ceiling(baud)
        bits_no_ts = 22 * 10;   % ~22 chars * 10 bits (8N1)
        bits_ts    = 26 * 10;   % ~26 chars * 10 bits when TS present
        ceil_no_ts = baud / bits_no_ts;
        ceil_ts    = baud / bits_ts;
    end
end
