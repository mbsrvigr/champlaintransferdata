process UPDATEDB {
    label 'process_single'
//    conda "${moduleDir}/environment.yml"
    
    input:
    tuple val(meta), val(info)
    path "dbinfo.yaml"
    path "webdb-cacert.pem"
    
    when:
    task.ext.when == null || task.ext.when

    script:
    def source_directory=meta.id
    def pi=info[1]
    """
    source ~/initConda.sh
    mamba activate champlain_env
    updateDB.py ${source_directory} ${pi} True "writer" ""
    """
}

