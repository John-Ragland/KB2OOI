% Compute IW perturbations for KB -> OOI
% uses iwGMtfast function (written by John Colosi)

function KB2OOI_real_func(time_idx)
    % Parameters
    % ----------
    % time_idx : int [0,42]
    %    integer multiple of MLS transmission

    % load loadEnvFile
    currentFileDir = fileparts(mfilename('fullpath'));
    root_dir = fileparts(fileparts(currentFileDir));
    env_vars = loadEnvFile(root_dir + "/.env");

    % check that path exists
    file_dir = env_vars.data_directory + "iws/time/sections";
    if ~isfolder(file_dir)
        % create directory if it doesn't exit
        mkdir(file_dir);
    end

    fno = sprintf(file_dir + "/dciw_%03i.mat", time_idx);

    % check that file doesn't already exist
    if isfile(fno)
        fprintf('file already exists, skipping...\n')
        return
    end

    % same realization seed
    seed = 0 * 81;

    % using LJ01D, because it is the longest path
    hydrophone = 'LJ01D';
    
    %% load data
    % open netcdf file
    fn = sprintf(env_vars.data_directory + "iws/KB_2_%s.nc", hydrophone);
    
    ni = ncinfo(fn);
    for i=1:length(ni.Variables)
        vn = ni.Variables(i).Name;
        tsc.(vn) = ncread(fn, vn);  % The result is a structure 
    end

    % compute N (N^2 is provided)
    tsc.N = sqrt(abs(tsc.N2));
    
    % change depth to be floats
    tsc.depth = double(tsc.depth);
    
    %% smooth profiles
    dz = double(tsc.depth(2) - tsc.depth(1));
    fN=2*pi/(2*dz);
    fc1=2*pi/100;
    Nb=4;
    Wn=fc1/fN;
    [B1,A1]=butter(Nb,Wn);
    
    tsc.Cf = filtfilt(B1,A1, tsc.C);
    tsc.Nf = filtfilt(B1,A1, tsc.N);
    
    %%
    % loop through range sections
    for idx=1:length(tsc.range)
        % new seed for every run
        seed = seed + 1;

        fprintf('computing for range %f km...\n', tsc.range(idx))
        zeta0 = 7.3;
        Nin = tsc.Nf(:,idx);
        Cin = tsc.Cf(:,idx);
        Zin = tsc.depth;
        latitude = tsc.lat(idx);
        time = time_idx * 27.28;

        jstar = 3;
        StrainThreshold = 0.3;
        dca = 0.0182;
        tic
        [ zetaiw, dciw, ziw, xiw, jmax ] = iwGMtfast(zeta0, Nin, Cin, Zin, latitude, time, seed, jstar, dca, StrainThreshold );
        toc
        
        x100km_idx = find(xiw > 110000);
        x100km_idx = x100km_idx(1);

        sectioniw(idx).zetaiw = zetaiw(:,1:x100km_idx);
        sectioniw(idx).dciw = dciw(:,1:x100km_idx);
        sectioniw(idx).ziw = ziw;
        sectioniw(idx).xiw = xiw(:,1:x100km_idx);
        sectioniw(idx).jmax = jmax;
       
    end


    save(fno, 'sectioniw', '-v7.3')

end
