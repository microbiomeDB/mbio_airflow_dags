#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)

if (length(args) < 1) {
    stop("Usage: Rscript mag_postProcessing.R <studyName> <output_dir>", call.=FALSE)
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

# install dplyr if not already installed
if (!requireNamespace("dplyr", quietly = TRUE))
    install.packages("dplyr")
    library(dplyr) # this one we attach bc i want the pipe %>% 

# install TreeSummarizedExperiment if not already installed
if (!requireNamespace("TreeSummarizedExperiment", quietly = TRUE)) {
    if (!requireNamespace("BiocManager", quietly = TRUE))
        install.packages("BiocManager")
    BiocManager::install("TreeSummarizedExperiment")
}

if (!requireNamespace(("mia"), quietly = TRUE)) {
    remotes::install_github('d-callan/mia@fix-importTaxpasta')
}

# install our own MicrobiomeDB if not already installed
if (!requireNamespace("MicrobiomeDB", quietly = TRUE))
    devtools::install_github("MicrobiomeDB/MicrobiomeDB")
############################################################################################

## create TreeSummarizedExperiment
## TODO need to modify this as other profilers get added
tse <- mia::importTaxpasta(paste0(outDir, "/taxprofiler_out/taxpasta/kraken2_kraken_db.biom"), addHierarchyTree = FALSE)
## these names should be the sra run ids
colnames(tse@assays@data[[1]]) <- veupathUtils::strSplit(colnames(tse@assays@data[[1]]), "_", 5, 3)

sampleMetadataFilePath <- file.path(dirname(outDir), "/sampleMetadata.tsv")
if (file.exists(sampleMetadataFilePath)) {
    sampleMetadata <- data.table::fread(sampleMetadataFilePath)
    names(sampleMetadata)[names(sampleMetadata) == 'name'] <- 'recordIDs'

    ## add sampleMetadata to TreeSummarizedExperiment
    SummarizedExperiment::colData(tse) <- S4Vectors::DataFrame(sampleMetadata, row.names = sampleMetadata$recordIDs)
}

## save TreeSummarizedExperiment as rda
save(tse, file=paste0(outDir, "/", studyName, "_treeSE.rda"))

## make an MbioDataset, should TSS normalize and keep raw values as well by default
dataset <- MicrobiomeDB::importTreeSummarizedExperiment(tse)

## preparing metatdenovo outputs
eggnogData <- data.table::fread(paste0(outDir, "/metatdenovo_out/summary_tables/megahit.prokka.emapper.tsv.gz"))

kofamData <- data.table::fread(paste0(outDir, "/metatdenovo_out/summary_tables/megahit.prokka.kofamscan.tsv.gz"))
tmp <- kofamData %>% group_by(orf) %>% summarize(evalue=min(evalue))
kofamData <- merge(tmp, kofamData, by = c('orf','evalue'))
kofamData$description <- paste0(kofamData$ko, ": ", kofamData$ko_definition)

geneCounts <- data.table::fread(paste0(outDir, "/metatdenovo_out/summary_tables/megahit.prokka.counts.tsv.gz"))
recordIds <- 'sample' # name of col in geneCounts w sample names

buildCollection <- function(dt, valueCol = c("count", "tpm"), collectionName) {
    combinedData <- merge(dt, geneCounts, by = "orf")
    combinedData$displayName <- gsub("[","(", combinedData$description, fixed=TRUE)
    combinedData$displayName <- gsub("]", ")", combinedData$displayName, fixed=TRUE)
    combinedData <- combinedData %>% group_by(displayName, sample) %>% summarize(value=sum(!!rlang::sym(valueCol)))

    ## tall to wide conversion
    combinedData <- reshape(as.data.frame(combinedData), idvar = recordIds, timevar = 'displayName', direction = 'wide')
    names(combinedData) <- gsub("value.", "", names(combinedData), fixed=TRUE)
    names(combinedData)[names(combinedData) == recordIds] <- 'recordIDs'

    collection <- veupathUtils::Collection(
        data = combinedData,
        recordIdColumn = 'recordIDs',
        ancestorIdColumns = character(0),
        name = collectionName
    )

    return(collection)
}

## NOTE: eggnog has ec numbers, reactions, pathways, etc also. 
## these could be incorporated later, its unclear the best path to do that, biologically
## these are often associated w multiple possible reactions and pathways
eggnogCountsCollection <- buildCollection(eggnogData, "count", "counts: eggnog functional annotation of orfs")
eggnogTpmCollection <- buildCollection(eggnogData, "tpm", "tpm: eggnog functional annotation of orfs")

kofamCountsCollection <- buildCollection(kofamData, "count", "counts: kofam scan functional annotation of orfs")
kofamTpmCollection <- buildCollection(kofamData, "tpm", "tpm: kofam scan functional annotation of orfs")

numExistingCollections <- length(dataset@collections)
dataset@collections[[numExistingCollections + 1]] <- eggnogCountsCollection
dataset@collections[[numExistingCollections + 2]] <- eggnogTpmCollection
dataset@collections[[numExistingCollections + 3]] <- kofamCountsCollection
dataset@collections[[numExistingCollections + 4]] <- kofamTpmCollection

validObject(dataset)

## save MbioDataset as rda
save(dataset, file=paste0(outDir, "/", studyName, "_mbioDataset.rda"))
