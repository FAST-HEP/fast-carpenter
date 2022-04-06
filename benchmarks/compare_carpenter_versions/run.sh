#!/usr/bin/env bash
REPO=github.com/kreczko/fast-carpenter.git

python -m pip install pipx
pipx install virtualenv

if [ ! -d HEPTutorial ]; then
    wget http://opendata.cern.ch/record/212/files/HEPTutorial_0.tar
    tar -xf HEPTutorial_0.tar HEPTutorial/files/
    rm HEPTutorial_0.tar
fi



# e.g. 1becc47 and 1b0912f
version1=$1
version2=$2

function version_lt()
{
    test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" != "$1";
}

echo "Comparing $version1 and $version2"

# set up versions
for version in $version1 $version2
do
    if [ ! -d "venv_${version}" ]
    then
        virtualenv -p python3 venv_$version
    fi
    source venv_$version/bin/activate
    echo "installing git+git://${REPO}@${version}"
    pip install --quiet git+git://$REPO@$version

    fc_version=$(fast_carpenter --version | cut -d ' ' -f2)
    tutorial_version=uproot4
    if version_lt $fc_version "0.20.0"
    then
        tutorial_version=uproot3
        # TODO: this needs some extra setup e.g. for coffea to work
    fi
    if [ ! -d "fast_cms_public_tutorial_${tutorial_version}" ]
    then
        git clone \
          -b kreczko-${tutorial_version} \
          git@github.com:FAST-HEP/FAST_cms_public_tutorial.git \
          fast_cms_public_tutorial_${tutorial_version}
        
        pip install --quiet -r fast_cms_public_tutorial_${tutorial_version}/requirements.txt
    fi
    
    mkdir -p output/${version}
done

# run versions
for version in $version1 $version2
do
    source venv_$version/bin/activate
    fc_version=$(fast_carpenter --version | cut -d ' ' -f2)
    tutorial_version=uproot4
    if version_lt $fc_version "0.20.0"
    then
        tutorial_version=uproot3
    fi
    echo "Running with commit=$version, fast_carpenter=$fc_version, tutorial=$tutorial_version"
    export PYTHONPATH=fast_cms_public_tutorial_${tutorial_version}:$PYTHONPATH

    time fast_carpenter \
      --mode="coffea:local" \
      --outdir output/${version}/ \
      fast_cms_public_tutorial_${tutorial_version}/file_list.yml \
      fast_cms_public_tutorial_${tutorial_version}/sequence_cfg.yml | tee output/${version}/log.txt

    time fast_plotter \
      -y log \
      -c fast_cms_public_tutorial_${tutorial_version}/plot_config.yml \
      -o output/${version}/plotter \
      output/${version}/tbl_dataset.*.csv | tee output/${version}/plotter_log.txt
done

# compare
diff -r output/${version1}/ output/${version2} | tee output/diff.txt
