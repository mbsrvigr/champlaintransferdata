process TRANSFER_DATA {
    label 'process_long'
    maxRetries 0

//    container 'docker://rbarrant/champlain:0.1'
    
    input:
    tuple val(meta), val(info)
    path "dbinfo.yaml"
    path "webdb-cacert.pem"

    output:
    tuple val(meta), val(info), emit: info

    when:
    task.ext.when == null || task.ext.when

    script:
    def source_directory=meta.id
    def target_directory=info[0]
    def pi=info[1]
    """
    source ~/initConda.sh
    mamba activate champlain_env
    transfer_data.py ${source_directory} ${target_directory} ${pi} True "writer" ""
    mamba deactivate
    """
}

