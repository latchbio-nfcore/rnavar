from dataclasses import dataclass
from enum import Enum
import os
import subprocess
import requests
import shutil
from pathlib import Path
import typing
import typing_extensions

from latch.resources.workflow import workflow
from latch.resources.tasks import nextflow_runtime_task, custom_task
from latch.types.file import LatchFile
from latch.types.directory import LatchDir, LatchOutputDir
from latch.ldata.path import LPath
from latch_cli.nextflow.workflow import get_flag
from latch_cli.nextflow.utils import _get_execution_name
from latch_cli.utils import urljoins
from latch.types import metadata
from flytekit.core.annotation import FlyteAnnotation

from latch_cli.services.register.utils import import_module_by_path

meta = Path("latch_metadata") / "__init__.py"
import_module_by_path(meta)
import latch_metadata

@custom_task(cpu=0.25, memory=0.5, storage_gib=1)
def initialize() -> str:
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if token is None:
        raise RuntimeError("failed to get execution token")

    headers = {"Authorization": f"Latch-Execution-Token {token}"}

    print("Provisioning shared storage volume... ", end="")
    resp = requests.post(
        "http://nf-dispatcher-service.flyte.svc.cluster.local/provision-storage",
        headers=headers,
        json={
            "storage_gib": 100,
        }
    )
    resp.raise_for_status()
    print("Done.")

    return resp.json()["name"]






@nextflow_runtime_task(cpu=4, memory=8, storage_gib=100)
def nextflow_runtime(pvc_name: str, input: LatchFile, outdir: typing_extensions.Annotated[LatchDir, FlyteAnnotation({'output': True})], email: typing.Optional[str], multiqc_title: typing.Optional[str], save_merged_fastq: typing.Optional[bool], genome: typing.Optional[str], fasta: typing.Optional[LatchFile], dict: typing.Optional[str], fasta_fai: typing.Optional[str], gtf: typing.Optional[str], gff: typing.Optional[str], exon_bed: typing.Optional[str], save_reference: typing.Optional[bool], known_indels: typing.Optional[str], known_indels_tbi: typing.Optional[str], dbsnp: typing.Optional[str], dbsnp_tbi: typing.Optional[str], snpeff_db: typing.Optional[str], vep_genome: typing.Optional[str], vep_species: typing.Optional[str], vep_cache_version: typing.Optional[str], star_index: typing.Optional[str], star_twopass: typing.Optional[bool], star_ignore_sjdbgtf: typing.Optional[bool], seq_center: typing.Optional[str], save_unaligned: typing.Optional[bool], save_align_intermeds: typing.Optional[bool], bam_csi_index: typing.Optional[bool], remove_duplicates: typing.Optional[bool], skip_baserecalibration: typing.Optional[bool], skip_intervallisttools: typing.Optional[bool], skip_variantfiltration: typing.Optional[bool], skip_variantannotation: typing.Optional[bool], skip_multiqc: typing.Optional[bool], no_intervals: typing.Optional[bool], read_length: typing.Optional[float], aligner: typing.Optional[str], star_max_memory_bamsort: typing.Optional[int], star_bins_bamsort: typing.Optional[int], star_max_collapsed_junc: typing.Optional[int], seq_platform: typing.Optional[str], gatk_hc_call_conf: typing.Optional[float], gatk_interval_scatter_count: typing.Optional[int], gatk_vf_window_size: typing.Optional[int], gatk_vf_cluster_size: typing.Optional[int], gatk_vf_fs_filter: typing.Optional[float], gatk_vf_qd_filter: typing.Optional[float]) -> None:
    try:
        shared_dir = Path("/nf-workdir")



        ignore_list = [
            "latch",
            ".latch",
            "nextflow",
            ".nextflow",
            "work",
            "results",
            "miniconda",
            "anaconda3",
            "mambaforge",
        ]

        shutil.copytree(
            Path("/root"),
            shared_dir,
            ignore=lambda src, names: ignore_list,
            ignore_dangling_symlinks=True,
            dirs_exist_ok=True,
        )

        cmd = [
            "/root/nextflow",
            "run",
            str(shared_dir / "main.nf"),
            "-work-dir",
            str(shared_dir),
            "-profile",
            "docker",
            "-c",
            "latch.config",
                *get_flag('input', input),
                *get_flag('outdir', outdir),
                *get_flag('email', email),
                *get_flag('multiqc_title', multiqc_title),
                *get_flag('save_merged_fastq', save_merged_fastq),
                *get_flag('genome', genome),
                *get_flag('fasta', fasta),
                *get_flag('dict', dict),
                *get_flag('fasta_fai', fasta_fai),
                *get_flag('gtf', gtf),
                *get_flag('gff', gff),
                *get_flag('exon_bed', exon_bed),
                *get_flag('read_length', read_length),
                *get_flag('save_reference', save_reference),
                *get_flag('known_indels', known_indels),
                *get_flag('known_indels_tbi', known_indels_tbi),
                *get_flag('dbsnp', dbsnp),
                *get_flag('dbsnp_tbi', dbsnp_tbi),
                *get_flag('snpeff_db', snpeff_db),
                *get_flag('vep_genome', vep_genome),
                *get_flag('vep_species', vep_species),
                *get_flag('vep_cache_version', vep_cache_version),
                *get_flag('aligner', aligner),
                *get_flag('star_index', star_index),
                *get_flag('star_twopass', star_twopass),
                *get_flag('star_ignore_sjdbgtf', star_ignore_sjdbgtf),
                *get_flag('star_max_memory_bamsort', star_max_memory_bamsort),
                *get_flag('star_bins_bamsort', star_bins_bamsort),
                *get_flag('star_max_collapsed_junc', star_max_collapsed_junc),
                *get_flag('seq_center', seq_center),
                *get_flag('seq_platform', seq_platform),
                *get_flag('save_unaligned', save_unaligned),
                *get_flag('save_align_intermeds', save_align_intermeds),
                *get_flag('bam_csi_index', bam_csi_index),
                *get_flag('remove_duplicates', remove_duplicates),
                *get_flag('gatk_hc_call_conf', gatk_hc_call_conf),
                *get_flag('skip_baserecalibration', skip_baserecalibration),
                *get_flag('skip_intervallisttools', skip_intervallisttools),
                *get_flag('skip_variantfiltration', skip_variantfiltration),
                *get_flag('skip_variantannotation', skip_variantannotation),
                *get_flag('skip_multiqc', skip_multiqc),
                *get_flag('gatk_interval_scatter_count', gatk_interval_scatter_count),
                *get_flag('no_intervals', no_intervals),
                *get_flag('gatk_vf_window_size', gatk_vf_window_size),
                *get_flag('gatk_vf_cluster_size', gatk_vf_cluster_size),
                *get_flag('gatk_vf_fs_filter', gatk_vf_fs_filter),
                *get_flag('gatk_vf_qd_filter', gatk_vf_qd_filter)
        ]

        print("Launching Nextflow Runtime")
        print(' '.join(cmd))
        print(flush=True)

        env = {
            **os.environ,
            "NXF_HOME": "/root/.nextflow",
            "NXF_OPTS": "-Xms2048M -Xmx8G -XX:ActiveProcessorCount=4",
            "K8S_STORAGE_CLAIM_NAME": pvc_name,
            "NXF_DISABLE_CHECK_LATEST": "true",
        }
        subprocess.run(
            cmd,
            env=env,
            check=True,
            cwd=str(shared_dir),
        )
    finally:
        print()

        nextflow_log = shared_dir / ".nextflow.log"
        if nextflow_log.exists():
            name = _get_execution_name()
            if name is None:
                print("Skipping logs upload, failed to get execution name")
            else:
                remote = LPath(urljoins("latch:///your_log_dir/nf_nf_core_rnavar", name, "nextflow.log"))
                print(f"Uploading .nextflow.log to {remote.path}")
                remote.upload_from(nextflow_log)



@workflow(metadata._nextflow_metadata)
def nf_nf_core_rnavar(input: LatchFile, outdir: typing_extensions.Annotated[LatchDir, FlyteAnnotation({'output': True})], email: typing.Optional[str], multiqc_title: typing.Optional[str], save_merged_fastq: typing.Optional[bool], genome: typing.Optional[str], fasta: typing.Optional[LatchFile], dict: typing.Optional[str], fasta_fai: typing.Optional[str], gtf: typing.Optional[str], gff: typing.Optional[str], exon_bed: typing.Optional[str], save_reference: typing.Optional[bool], known_indels: typing.Optional[str], known_indels_tbi: typing.Optional[str], dbsnp: typing.Optional[str], dbsnp_tbi: typing.Optional[str], snpeff_db: typing.Optional[str], vep_genome: typing.Optional[str], vep_species: typing.Optional[str], vep_cache_version: typing.Optional[str], star_index: typing.Optional[str], star_twopass: typing.Optional[bool], star_ignore_sjdbgtf: typing.Optional[bool], seq_center: typing.Optional[str], save_unaligned: typing.Optional[bool], save_align_intermeds: typing.Optional[bool], bam_csi_index: typing.Optional[bool], remove_duplicates: typing.Optional[bool], skip_baserecalibration: typing.Optional[bool], skip_intervallisttools: typing.Optional[bool], skip_variantfiltration: typing.Optional[bool], skip_variantannotation: typing.Optional[bool], skip_multiqc: typing.Optional[bool], no_intervals: typing.Optional[bool], read_length: typing.Optional[float] = 151, aligner: typing.Optional[str] = 'star', star_max_memory_bamsort: typing.Optional[int] = 0, star_bins_bamsort: typing.Optional[int] = 50, star_max_collapsed_junc: typing.Optional[int] = 1000000, seq_platform: typing.Optional[str] = 'illumina', gatk_hc_call_conf: typing.Optional[float] = 20, gatk_interval_scatter_count: typing.Optional[int] = 25, gatk_vf_window_size: typing.Optional[int] = 35, gatk_vf_cluster_size: typing.Optional[int] = 3, gatk_vf_fs_filter: typing.Optional[float] = 30, gatk_vf_qd_filter: typing.Optional[float] = 2) -> None:
    """
    nf-core/rnavar

    Sample Description
    """

    pvc_name: str = initialize()
    nextflow_runtime(pvc_name=pvc_name, input=input, outdir=outdir, email=email, multiqc_title=multiqc_title, save_merged_fastq=save_merged_fastq, genome=genome, fasta=fasta, dict=dict, fasta_fai=fasta_fai, gtf=gtf, gff=gff, exon_bed=exon_bed, read_length=read_length, save_reference=save_reference, known_indels=known_indels, known_indels_tbi=known_indels_tbi, dbsnp=dbsnp, dbsnp_tbi=dbsnp_tbi, snpeff_db=snpeff_db, vep_genome=vep_genome, vep_species=vep_species, vep_cache_version=vep_cache_version, aligner=aligner, star_index=star_index, star_twopass=star_twopass, star_ignore_sjdbgtf=star_ignore_sjdbgtf, star_max_memory_bamsort=star_max_memory_bamsort, star_bins_bamsort=star_bins_bamsort, star_max_collapsed_junc=star_max_collapsed_junc, seq_center=seq_center, seq_platform=seq_platform, save_unaligned=save_unaligned, save_align_intermeds=save_align_intermeds, bam_csi_index=bam_csi_index, remove_duplicates=remove_duplicates, gatk_hc_call_conf=gatk_hc_call_conf, skip_baserecalibration=skip_baserecalibration, skip_intervallisttools=skip_intervallisttools, skip_variantfiltration=skip_variantfiltration, skip_variantannotation=skip_variantannotation, skip_multiqc=skip_multiqc, gatk_interval_scatter_count=gatk_interval_scatter_count, no_intervals=no_intervals, gatk_vf_window_size=gatk_vf_window_size, gatk_vf_cluster_size=gatk_vf_cluster_size, gatk_vf_fs_filter=gatk_vf_fs_filter, gatk_vf_qd_filter=gatk_vf_qd_filter)

