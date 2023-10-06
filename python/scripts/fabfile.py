from fabric import task
from fabric.connection import Connection
from patchwork.files import exists
from invoke import Exit
from configparser import ConfigParser
import os

from sndaq.logging import get_logger

logger = get_logger(name='pysndaq_fabric', log_path=os.path.split(__file__)[0])

_config = ConfigParser()
_config.read('fabfile.cfg')

@task
def help(ctx):
    print("Installer help message")


def git_clone_pysndaq(ctx, target, istag, src_path, githost):
    # Check if the source path already exists
    if not os.path.isdir(src_path):
        logger.debug(f"Creating directory {src_path} for initial git Clone")
        ctx.run(f"mkdir -p {src_path}")

    with ctx.cd(src_path):
        # Check if a git repo already exists
        if not os.path.isdir(src_path + "/.git"):
            logger.debug(f"{os.path.join(src_path,'.git')} Not found.")
            logger.info("Cloning repository")
            cmd = f"git clone --depth 1 {githost:s} ."  # Shallow clone to reduce bandwidth usage
            response = ctx.run(cmd)
            if response.failed:
                msg = "Failed to clone repository from GitHub"
                logger.error(msg)
                Exit(msg)

        # Fetch the target using a release tag
        if istag:
            ctx.run(f"git fetch --depth 1 origin tag {target}")
            ctx.run(f"git checkout {target}")

        # Fetch the target using a banch
        else:
            # Ensure the branch exists locally
            response = ctx.run(f"git rev-parse --verify {target}", warn=True)

            # If the branch doesn't exist locally, fetch it from the remote
            if response.failed:
                # Add branch to tracked remotes (Initial clone above is shallow)
                ctx.run(f"git remote set-branches origin {target}", warn=True)

                # Fetch last change to branch (reduces data transfer for low latency system)
                ctx.run(f"git fetch --depth 1 origin {target}", warn=True)

                # Switch to new branch
                ctx.run(f"git checkout --track origin/{target}", warn=True)

            # If the branch does exist locally, check it out and update it
            else:
                ctx.run(f"git checkout {target}", warn=True)
                ctx.run("git pull --ff-only", warn=True)


def parse_git_version(version):
    if version.startswith("branches/"):
        sndaq_version = str.replace(version, "/", "-", 1)
        istag = False
    else:
        sndaq_version = version
        istag = True
    sndaq_revision = "latest"
    return sndaq_version, sndaq_revision, istag


def confirm(question):
    resp_yes = ['y', 'yes']
    resp_no = ['n', 'no']
    print(question)
    n_attempts = 5
    while n_attempts > 0:
        response = input('Proceed? (y/n): ')
        if not response or response in resp_no:
            return False
        elif response.lower() in resp_yes:
            return True
        else:
            print(f'Invalid input `{response}`. Must be one of: {resp_yes + resp_no}. Try again. '
                  f'({n_attempts} Left)')
        n_attempts -= 1
    msg = 'Exceeded maximum allowed attempts'
    print(msg)
    return False


@task
def stage(ctx, target='branches/main', stage_path=None):
    ctx.config.run.replace_env = False  # Keep shell env

    # Check that the git working area already exists locally
    if stage_path is None:
        stage_path = os.path.abspath(_config.get('stage', 'stage_path'))
    githost = _config.get('stage', 'githost')

    # TODO: Shouldn't this check for existence first?
    git_path = f"{stage_path}/sndaq-git"
    with ctx.cd(git_path):
        ctx.run('git pull', warn=True)  # TODO: Remove this for sake of bandwidth
        version, revision, istag = parse_git_version(target)
        if not istag:
            branch = str.replace(version, "branches-", "", 1)
        else:
            branch = version

    version_string = f"{version:s}_{revision:s}"

    hostname = ctx.run("hostname -f").stdout
    user = ctx.run("whoami").stdout

    # Ensure the environment is correct, detect if on SPS, SPTS, or other
    if "access" not in hostname:
        if not confirm("Not staging on 'access', continue?"):
            print("Exiting on user request")
            raise SystemExit

    if "spts" in hostname or "icecube.wisc.edu" in hostname:
        cluster = "SPTS"
    elif "sps" in hostname or "icecube.southpole.usap.gov" in hostname:
        cluster = "SPS"
        logger.info("ATTENTION: Installation on SPS.")
    else:
        cluster = "SPTS"
        if not confirm("Installing on unknown system, continue?"):
            print("Exiting on user request")
            raise SystemExit
        else:
            logger.info(f"Installing on unknown system as user {user:s}. Setting CLUSTER as SPTS")

    logger.info(f"Staging PySNDAQ version {version:s} rev {revision:s}")

    rev_path = "/".join([stage_path, f"sndaq_{version}"])

    # Create the directories
    ctx.run(f"mkdir -p {rev_path:s}")
    logger.info(f"Checking out PySNDAQ into {rev_path:s}")
    git_clone_pysndaq(ctx, branch, istag, git_path, githost)

    # Copy files into rev_path
    for subdir in ('python', 'data', 'setup.py', 'requirements.txt'):
        logger.debug(f'Copying {os.path.join(git_path,subdir)} to Revision staging area')
        ctx.run(f"cp -r {os.path.join(git_path,subdir)} {rev_path}")
    ctx.run(f"mkdir -p {os.path.join(git_path, 'log')}")

    # Run CMake, build, and install
    logger.info(f"Running setup.py in staging directory {rev_path}")
    with ctx.cd(rev_path):
        ctx.run('pip install -e .')
    # May not be needed here, but this should compile the pybind extension
    # In which case it will need to either be re-run on 2ndbuild, or the compilation should proceed
    # standalone here before copying the .so over.



@task
def deploy(ctx, stage_path, deploy_target, deploy_path, control_target, control_path):
    host = ctx.run("hostname -f").stdout
    user = ctx.run("whoami").stdout

    ctx.run(' '.join(['rsync -ar',
                      f'{stage_path:s}/',
                      f'{deploy_target}:{deploy_path}/']))

    with Connection(deploy_target) as ctx_deploy:
        required_dirs = ('log',)
        for directory in required_dirs:
            if not exists(ctx_deploy, os.path.join(deploy_path, directory)):
                ctx_deploy(f"mkdir -p {deploy_path}/{directory}")

    with Connection(control_target) as ctx_control:
        pass
