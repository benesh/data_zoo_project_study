from prefect.filesystems import GitHub

block = GitHub(repository="https://github.com/benesh/data_zoo_project_study/")
block.save("block-git-access")