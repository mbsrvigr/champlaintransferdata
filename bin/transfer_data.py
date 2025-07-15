#!/usr/bin/env python

"""
Name:
    transferData.py

Authors:
    Ramiro Barrantes-Reynolds
    Patrick Harvey

Description:
    This script moves a directory from one location to another,
    logs the move in a database and validates the transfer
    using MD5 checksums.
"""

# Setup
from argparse import ArgumentParser
from genericpath import isdir
import hashlib
import os
import datetime
import shutil
import subprocess
from typing import Literal
import yaml
import copy
from sqlalchemy import create_engine, Column, DateTime
from sqlalchemy import Boolean, Integer, String, BigInteger
from sqlalchemy.orm import sessionmaker, DeclarativeBase

# Constants
DB_CONFIG = "./dbinfo.yaml"


# Class Definitions
class Base(DeclarativeBase):
    """
    Base object for creating data transfer objects.
    """

    pass


class Transfer(Base):
    """
    Data transfer class to record file transfers.
    """

    __tablename__ = "dataTransfers"
    id = Column("id", Integer, primary_key=True)
    date = Column(
        "date",
        DateTime,
        default=datetime.datetime.now(tz=datetime.timezone.utc),
    )
    source_directory = Column(
        "source_directory",
        String(length=255),
        unique=False,
        nullable=False,
    )
    target_directory = Column(
        "target_directory",
        String(length=255),
        unique=False,
        nullable=False,
    )
    file_name = Column(
        "file_name",
        String(length=255),
        unique=False,
        nullable=False,
    )
    pi = Column("pi", String(length=100), unique=False, nullable=False)
    size_in_bytes = Column("size_in_bytes", BigInteger, unique=False, nullable=False)
    md5_check = Column("md5_check", Boolean, unique=False, default=True)
    remarks = Column("remarks", String(length=2048), unique=False, nullable=False)

    def copy(self):
        return copy.copy


# Function Definitions
def db_connect(local=False, role="writer"):
    """
    Connect to the database with specified role.
    """
    with open(file=DB_CONFIG, encoding="utf8") as stream:
        try:
            config = yaml.safe_load(stream=stream)
        except yaml.YAMLError as exc:
            print(exc)

    if local:
        ssl_args = {"ssl": {"ca": config["loc_ca"]}}
    else:
        ssl_args = {"ssl": {"ca": config["web_ca"]}}

    password = config.get(role, "writer")
    if password == "mbsr_writer":
        password = config["w_password"]
    elif password == "mbsr_admin":
        password = config["a_password"]
    elif password == "mbsr_reader":
        password = config["r_password"]
    else:
        password = ""

    db_engine = create_engine(
        url=f"mysql+pymysql://{config[role]}:{password}@{config['host']}/{config['database']}",
        connect_args=ssl_args,
    )
    Transfer.metadata.create_all(bind=db_engine)
    Session = sessionmaker(bind=db_engine)
    db = Session()
    return db


def net_connect():
    """
    Connect to the network files.
    """
    with open(file=DB_CONFIG, encoding="utf8") as stream:
        try:
            config = yaml.safe_load(stream=stream)
        except yaml.YAMLError as exc:
            print(exc)
        vacc_un = config["vacc_un"]
        vacc_pw = config["vacc_pw"]
    return (vacc_un, vacc_pw)


def source_meta(file_data: Transfer, file_in: str):
    """
    Record metadata of the source directory.
    """
    file_data.date = str(datetime.datetime.now())[0:19]
    file_data.size_in_bytes = compute_directory_size(file_in)
    file_data.file_name = file_in
    print(file_data.size_in_bytes)
    return file_data


def target_find(source_dir: str, target_base_dir: str):
    """
    Check if source base directory exists and if target directory exists.
    """
    source_dir = source_dir.rstrip("/")
    if not os.path.exists(target_base_dir):
        print(f"Error: Target base directory '{target_base_dir}' does not exist.")
        exit(1)
    target_dir = target_base_dir + "/" + os.path.basename(source_dir)
    if os.path.exists(target_dir):
        print(f"Warning: Target directory '{target_dir}' already exists.")
        return target_dir
    else:
        return target_dir

def runsh(command: str):
    """
    Run a shell command.
    """
    return subprocess.run(command,
                          shell=True,
                          text=True,
                          capture_output=True)



def compute_md5_for_dir(source_dir: str,
                        target_dir: str):
    """
    Compute MD5 checksums for all files in a given directory.
    """
    print(source_dir)
    md5_list_file = target_dir + "/" + os.path.basename(os.path.normpath(source_dir)) + ".md5"
    print(md5_list_file)
    runsh(f"cd {source_dir}; find . -type f -print0 | xargs -0 md5sum >> {md5_list_file}")
    md5_result_file = target_dir + "/md5_check_result.txt"
    runsh(f"cd {target_dir}; md5sum -c {md5_list_file} > {md5_result_file}")
    nLines=int(runsh(f"wc -l < {md5_result_file}").stdout)
    nOKs = int(runsh(f"grep 'OK' {md5_result_file} | wc -l").stdout)
    print(f"VALIDATION: {nLines} transferred, {nOKs} matching md5s (they should be the same number).") 
    if nLines!=nOKs:
        print(f"DATA TRANSFER CORRUPT. SOURCE: {source_dir}; TARGET: {target_dir}")
        exit(1)


def compute_directory_size(directory: str):
    """
    Compute the size of directory in bytes
    """
    print(directory)
    size=runsh(f"du -sb {directory} | perl -p -e 's/(\\d+).*/\\1/'").stdout
    return int(size)

def record_trans(logfile: sessionmaker, filedata: Transfer):
    """
    Record the transfer in the database.
    """
    logfile.add(filedata)
    logfile.commit()


def source_del(check: bool, path_in: str, disp: bool):
    """
    Delete the source file if check is True.
    """
    if check:
#        subprocess.check_output(f"rm -r {path_in}", stderr=subprocess.STDOUT, shell=True)
        if disp:
            print("SOURCE deleted")
            print(path_in)


def source_to_target(
    path_in: str,
    path_out: str,
    disp: bool,
    file_trans: Transfer,
    log_file: sessionmaker,
):
    """
    Copy files from the source to the target.
    TODO: reformat to copy entire directory and check md5sum across files.
    """
    # vacc = net_connect()
    # os.environ["RSYNC_PASSWORD"] = vacc[1]
    # init_dir = os.getcwd()
    # if init_dir.split("/")[0] != path_in.split("/")[0]:
    #     os.chdir("/")
    try:
        print(f"Copying {path_in} to {path_out}")
        shutil.copytree(path_in, path_out)
    except RuntimeError as error:
        print(error)
        print("The linux_interaction() function wasn't executed.")

#except FileExistsError:
#        print(f"Directory {path_out} already exists." + "\n")
#        pass

    compute_md5_for_dir(path_in, path_out)

    file_trans = source_meta(file_trans, path_in)
    
#    record_trans(log_file, file_trans)

#    source_del(True, path_in, True)


def main():
    """
    Description:
        Main function to run the script.

    Arguments:
        source_directory (str):
            Path to the source directory.
        target_directory (str):
            Path to the target directory.
        pi_netid (str):
            netid of the principal investigator.
        display (Boolean):
            Displays results to console.

    Example Usage (not run):

    $ python transfer_data.py "/netfiles02/mbsr2/AGTC_SingularG4/240321_mhenders_18736_pool3_FC1" "/netfiles/jdragon/bullhead_data" "jdragon" True "writer"

    """
    parser = ArgumentParser(
        description="Move a source directory into a target directory."
    )
    parser.add_argument(
        "source_directory", type=str, help="Path to the source directory. (str: any)"
    )
    parser.add_argument(
        "target_directory", type=str, help="Path to the target directory. (str: any)"
    )
    parser.add_argument(
        "pi_netid", type=str, help="netid of the principal investigator. (str: any)"
    )
    parser.add_argument(
        "display",
        type=bool,
        help="displays results to console. (Boolean: True | [False])",
    )
    parser.add_argument(
        "db_role", type=str, help="DB role to use. (str: [writer] | reader | admin)"
    )
    parser.add_argument(
        "db_remark", type=str, help="Any note about the transfer, in quotes", default=""
    )

    args = parser.parse_args()
    d_in = args.source_directory.rstrip("/")
    d_out = target_find(d_in,args.target_directory)
    d_pid = args.pi_netid
    d_remark = args.db_remark

    d_disp = args.display
    if d_disp not in [True, False]:
        d_disp = False

    d_role = args.db_role
    if d_role not in ["writer", "reader", "admin"]:
        d_role = "writer"

    db_log = db_connect(local=False, role=d_role)
    print(f"Transferring {d_in} into {d_out}")
    
    d_data = Transfer(
        source_directory=f"{d_in}",
        target_directory=f"{d_out}",
        pi=d_pid,
        md5_check=True,
        remarks=d_remark
    )

    source_to_target(
        path_in=d_in, path_out=d_out, disp=d_disp, file_trans=d_data, log_file=db_log
    )


if __name__ == "__main__":
    main()
