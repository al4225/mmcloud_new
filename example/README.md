## Example `mm_interactive.sh` commands

Here we assume that 

1. the data to be analyzed are already uploaded to S3 bucket by [the Data Admin](https://wanggroup.org/productivity_tips/memverge-aws#notes-for-data-admin).
2. the analysis script is also available on S3 --- in this example the [xqtl-pipeline repo](https://github.com/cumc/xqtl-pipeline) is cloned to the bucket.
3. the FSx or EFS volume already has all necessary software installed

We use the command below to submit commands in `commands_to_submit.txt`.

### Example Batch Submission

```bash
username=aw3600
./src/mm_interactive.sh \
 -o 3.82.198.55 \
 -g g-sidlpgb7oi9p48kxycpmn \
 -sg sg-02867677e76635b25 \
 -efs 10.1.10.210 \
 --job-script ./example/commands_to_submit.txt \
 --mount-packages \
 --job-size 100 \
 --mount "statfungen/ftp_fgc_xqtl:/home/$username/data,statfungen/ftp_fgc_xqtl/sos_cache/$username:/home/$username/.sos,statfungen/ftp_fgc_xqtl/analysis_result/finemapping_twas:/home/$username/output" \
 --mountOpt mode=r,mode=rw,mode=rw \
 --cwd "/home/$username/data" \
 --download "statfungen/ftp_fgc_xqtl/ROSMAP/genotype/analysis_ready/geno_by_chrom/:/home/$username/input/" \
 --download-include "ROSMAP_NIA_WGS.leftnorm.bcftools_qc.plink_qc.1.*" \
 --ebs-mount "/home/$username/input=60" \
 -jn example_job \
 --no-fail-fast  
```

To explain the parameters,
- `-o 3.82.198.55` is the Opcenter IP. This is where submitted jobs will be sent to and can be viewed from.
- `-g g-sidlpgb7oi9p48kxycpmn` and `-sg sg-02867677e76635b25` are gateway ID and security group, respectively. You can ask your admin for these IDs. These help in the VM's networking.
- `-efs 10.1.10.210` specifies the IP of the EFS used in order to access installed packages.
- `--job-script ./example/commands_to_submit.txt` provides the actual commands we want to submit to the VM. Providing this specifies batch mode
- `--oem-packages` and `--mount-packages` are two modes that specify how the user can use certain packages. The former allows the user to use shared packages, and the latter allow the user to use user-installed packages. One and only one can be used.
- `-c 2` and `-m 16` specifies that the VM should have 2 CPU threads and 16GB of memory.
- `--job-size 100` will split commands per line within `commands_to_submit.txt` into batches, each batch has at most 100 commands.
- `--mount` includes three folders: the AWS folder `s3://statfungen/ftp_fgc_xql` is mounted to the VM as `~/data`; the AWS folder `s3://statfungen/ftp_fgc_xqtl/sos_cache/aw3600` is mounted to the VM as `~/.sos`; the AWS folder `statfungen/ftp_fgc_xqtl/analysis_result/finemapping_twas` is mounted to the VM as `~/output`. Notice how they are comma-separated.
- `--mountOpts` specifies "mode=r" for the first folder that mounts it as read-only to the analysis command. That means the analysis command cannot directly change or add anything to `~/data` folder in the VM. The second folder is mounted with "mode=rw", that is, the analysis command can write into the `~/.sos` folder in the VM. The third folder is mounted with "mode=rw", so we can directly write the outputs to that folder as they are generated. Notice how they are comma-separated.
- `--download` specifies the folder inside of the S3 bucket that we would like to download to the VM, at the begin of the analysis. If any data has been downloaded using this command, you should update the file paths in the 'commands_to_submit.txt' file accordingly. And **add `/` after the local folder in download** (because we want to download into a folder). For instance, if we downloaded genotype data from `statfungen/ftp_fgc_xqtl/ROSMAP/genotype/analysis_ready/geno_by_chrom/` to the VM at `/home/$username/input/`, then the genotype data path in your 'commands_to_submit.txt' should be specified as `../input`.
- `--download-include` should be used to specify the prefix or suffix of files you want to download from S3 bucket. 
- `--ebs-mount` Mount a dedicated local EBS volume to the VM instance. When downloading data from an S3 bucket instead of using direct mounts, ensure you allocate sufficient storage space to the destination path by mounting a dedicated EBS volume. It must be different from the path in `--mount` which mounts a folder on the S3 bucket. 
- `-jn` is the job name of the batch job. If a job name is specified, a number suffix will be added to the job name. For example, if there were 10 jobs submitted with this command, you will see job names from `example_job_1` to `example_job_10`.
- `--no-fail-fast` when this switch is turned on, all commands in a batch will be executed regardless if the previous ones failed or succeeded. 

To test this for yourself without submitting the job, please add `--dryrun` to the end of the command (eg right after `--no-fail-fast`) and run on your computer. You should find a file called `commands_to_submit_1.mmjob.sh` you can take a look at it to see the actual script that will be executed on the VM.

### Example Interactive submission
```bash
./src/mm_interactive.sh \
 -g g-sidlpgb7oi9p48kxycpmn \
 -sg sg-02867677e76635b25 \
 -efs 10.1.10.210 \
 --mount-packages \
 -jn TEST_ROCKEFELLER_oem_mount_packages \
 -ide juypter
```

Some of these parameters are shared with the batch job above. They will be skipped in the following explanation:
- `--oem-packages` and `--mount-packages` are two modes that specify how the user can use certain packages. The former allows the user to use shared packages, and the latter allow the user to use user-installed packages. One and only one can be used.
- `-jn` is the job name of the interactive job. By default, the name of the interactive job would be `<user>_<ide>_<port>`.
- `-ide jupyter` specifies the ide used for the interactive job and providing this specifies an interactive job. By default, it will use the shell session `tmate`, however, `jupyter`, `vscode`, and `rstudio` can be used.
