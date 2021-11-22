/*
========================================================================================
    RECALIBRATE
========================================================================================
*/

params.applybqsr_options      = [:]
params.merge_bam_options      = [:]
params.qualimap_bamqc_options = [:]
params.samtools_index_options = [:]
params.samtools_stats_options = [:]

include { GATK4_APPLYBQSR as APPLYBQSR } from '../../modules/nf-core/modules/gatk4/applybqsr/main' addParams(options: params.applybqsr_options)
include { SAMTOOLS_INDEX }               from '../../modules/nf-core/modules/samtools/index/main'  addParams(options: params.samtools_index_options)
include { SAMTOOLS_MERGE }               from '../../modules/nf-core/modules/samtools/merge/main'  addParams(options: params.merge_bam_options)
include { SAMTOOLS_STATS }               from '../../modules/nf-core/modules/samtools/stats/main'  addParams(options: params.samtools_stats_options)

workflow RECALIBRATE {
    take:
        skip_bamqc     // boolean: true/false
        skip_samtools  // boolean: true/false
        bam            // channel: [mandatory] bam
        dict           // channel: [mandatory] dict
        fai            // channel: [mandatory] fai
        fasta          // channel: [mandatory] fasta
        intervals      // channel: [mandatory] intervals

    main:

    ch_versions = Channel.empty()

    bam_recalibrated_index = Channel.empty()
    bam_recalibrated       = Channel.empty()
    bam_reports            = Channel.empty()

    APPLYBQSR(bam, fasta, fai, dict, intervals)
    ch_versions = ch_versions.mix(APPLYBQSR.out.versions.first())

    // STEP 4.5: MERGING AND INDEXING THE RECALIBRATED BAM FILES
    if (params.no_intervals) {
        bam_recalibrated = APPLYBQSR.out.bam
    } else {
        APPLYBQSR.out.bam.map{ meta, bam ->
            meta.id = meta.id
            [meta, bam]
        }.groupTuple().set{bam_recalibrated_interval}

        SAMTOOLS_MERGE(bam_recalibrated_interval, fasta)
        bam_recalibrated = SAMTOOLS_MERGE.out.bam
        ch_versions = ch_versions.mix(SAMTOOLS_MERGE.out.versions.first())
    }

    SAMTOOLS_INDEX(bam_recalibrated)
    bam_recalibrated_index = bam_recalibrated.join(SAMTOOLS_INDEX.out.bai)
    ch_versions = ch_versions.mix(SAMTOOLS_INDEX.out.versions.first())

    samtools_stats = Channel.empty()

    if (!skip_samtools) {
        SAMTOOLS_STATS(bam_recalibrated_index, [])
        samtools_stats = SAMTOOLS_STATS.out.stats
        ch_versions = ch_versions.mix(SAMTOOLS_STATS.out.versions.first())
    }
    bam_reports = samtools_stats


    emit:
        bam         = bam_recalibrated_index
        qc          = bam_reports

        versions    = ch_versions

}
