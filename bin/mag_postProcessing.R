#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)

if (length(args) < 1) {
    stop("Usage: Rscript metatdenovo_postProcessing.R <output_dir>", call.=FALSE)
}

studyName <- args[1]
outDir <- args[2]

if (!dir.exists(outDir))
    stop("output directory does not exist: ", outDir)


## TODO consider asking for specific versions of these packages...
############################################################################################
# install data.table if not already installed
if (!requireNamespace("data.table", quietly = TRUE))
    install.packages("data.table")

# install TreeSummarizedExperiment if not already installed
if (!requireNamespace("TreeSummarizedExperiment", quietly = TRUE)) {
    if (!requireNamespace("BiocManager", quietly = TRUE))
        install.packages("BiocManager")
    BiocManager::install("TreeSummarizedExperiment")
}

if (!requireNamespace(("mia"), quietly = TRUE)) {
    if (!requireNamespace("BiocManager", quietly = TRUE))
        install.packages("BiocManager")
    BiocManager::install("mia")
}

# install our own MicrobiomeDB if not already installed
if (!requireNamespace("MicrobiomeDB", quietly = TRUE))
    devtools::install_github("MicrobiomeDB/MicrobiomeDB")
############################################################################################

## create TreeSummarizedExperiment
## TODO need to modify this as other profilers get added
tse <- mia::importTaxpasta(paste0(outDir, "taxprofiler_out/taxpasta/kraken2_kraken_db.biom"))

if (dir.exists(paste0(outDir, "/sampleMetadata.tsv"))) {
    sampleMetadata <- data.table::fread(paste0(outDir, "/sampleMetadata.tsv"))
    names(sampleMetadata)[names(sampleMetadata) == 'name'] <- 'recordIDs'

    ## add sampleMetadata to TreeSummarizedExperiment
    colData(tse) <- sampleMetadata
}

## save TreeSummarizedExperiment as rda
save(tse, file=paste0(outDir, studyName, "_treeSE.rda"))

## make an MbioDataset, should TSS normalize and keep raw values as well by default
dataset <- MicrobiomeDB::importTreeSE(tse)

## TODO add metatdenovo output to MbioDataset
## TODO file and col names are placeholders, based roughly on my memory
eggnogData <- data.table::fread(paste0(outDir, "/metatdenovo_out/summarized_data/eggnog.tsv"))
kofamData <- data.table::fread(paste0(outDir, "/metatdenovo_out/summarized_data/kofam.tsv"))
geneCounts <- data.table::fread(paste0(outDir, "/metatdenovo_out/summarized_data/gene_counts.tsv"))
recordIds <- names(geneCounts)[!names(geneCounts) %in% c("orf", "count", "tpm")]

buildCollection <- function(dt, valueCol = c("count", "tpm"), collectionName) {
    combinedData <- data.table::merge(dt, geneCounts, by = "orf")
    combinedData$displayName <- paste0(combinedData$orf, " - ", combinedData$description)
    keepCols <- c(recordIds, "displayName", valueCol)
    combinedData <- combinedData[, keepCols, with = FALSE]

    ## tall to wide conversion
    combinedData <- data.table::dcast(combinedData, displayName ~ recordIds, value.var = c(valueCol))

    collection <- veupathUtils::Collection(
        data = combinedData,
        recordIdColumn = 'recordIDs',
        ancestorIdColumns = character(0),
        name = collectionName
    )

    validObject(collection)

    return(collection)
}

eggnogCountsCollection <- buildCollection(eggnogData, "count", "eggnog annotated gene counts")
eggnogTpmCollection <- buildCollection(eggnogData, "tpm", "eggnog annotated gene tpm")

## TODO i think kofam gives ec numbers and pathways, not just descriptions
## we should maybe include those as separate collections
kofamCountsCollection <- buildCollection(kofamData, "count", "kofam annotated gene counts")
kofamTpmCollection <- buildCollection(kofamData, "tpm", "kofam annotated gene tpm")

numExistingCollections <- length(dataset@collections)
dataset@collections[[numExistingCollections + 1]] <- eggnogCountsCollection
dataset@collections[[numExistingCollections + 2]] <- eggnogTpmCollection
dataset@collections[[numExistingCollections + 3]] <- kofamCountsCollection
dataset@collections[[numExistingCollections + 4]] <- kofamTpmCollection

## TODO add geNomad, etc from mag output to MbioDataset
### looking at mag outputs, it doesnt seem to have run geNomad for some reason..

validObject(dataset)

## save MbioDataset as rda
save(dataset, file=paste0(outDir, studyName, "_mbioDataset.rda"))