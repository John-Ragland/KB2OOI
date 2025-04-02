function env = loadEnvFile(filepath)
    % Initialize empty struct for environment variables
    env = struct();
    
    % Open the .env file
    fileID = fopen(filepath, 'r');
    
    % Check if file opened successfully
    if fileID == -1
        error('Could not open file: %s', filepath);
    end
    
    % Read file line by line
    line = fgetl(fileID);
    while ischar(line)
        % Skip empty lines and comments
        if ~isempty(line) && line(1) ~= '#'
            % Split by the first equals sign
            parts = split(line, '=', 2);
            if length(parts) == 2
                key = strtrim(parts{1});
                value = strtrim(parts{2});
                
                % Remove quotes if present
                if (startsWith(value, '"') && endsWith(value, '"')) || ...
                   (startsWith(value, '''') && endsWith(value, ''''))
                    value = value(2:end-1);
                end
                
                % Store in struct
                env.(key) = value;
            end
        end
        line = fgetl(fileID);
    end
    
    % Close the file
    fclose(fileID);
end