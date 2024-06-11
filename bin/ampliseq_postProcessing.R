#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)

if (length(args) < 1) {
    stop("Usage: Rscript ampliseq_postProcessing.R <output_dir>", call.=FALSE)
}

studyName <- args[1]
outDir <- args[2]
referenceDB <- args[3]
if (is.na(referenceDB)) {
    referenceDB <- 'silva_138'
}

if (!dir.exists(outDir))
    stop("output directory does not exist: ", outDir)

if (!dir.exists(paste0(outDir, "/dada2")))
    stop("dada2 directory does not exist: ", paste0(outDir, "/dada2"))

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

# install our own MicrobiomeDB if not already installed
if (!requireNamespace("MicrobiomeDB", quietly = TRUE))
    devtools::install_github("MicrobiomeDB/MicrobiomeDB")
############################################################################################

## first get rowData
rowData <- data.table::fread(paste0(outDir, "/dada2/ASV_tax_species.", referenceDB, ".tsv"))
row.names(rowData) <- rowData$ASV_ID
rowData$ASV_ID <- NULL
# remove confidences (we don't necessarily need them once theyre filtered on)
rowData$confidence <- NULL

## then get assay data
assayData <- data.table::fread(paste0(outDir, "/dada2/ASV_table.tsv"))
row.names(assayData) <- assayData$ASV_ID
assayData$ASV_ID <- NULL

sampleMetadata <- data.table::fread(paste0(outDir, "/sampleMetadata.tsv"))
names(sampleMetadata)[names(sampleMetadata) == 'name'] <- 'recordIDs'

## create TreeSummarizedExperiment
# may have to add picrust results as colData
tse <- TreeSummarizedExperiment::TreeSummarizedExperiment(
    assays = list('Counts'=assayData), 
    rowData = rowData, 
    colData = sampleMetadata
)

## save TreeSummarizedExperiment as rda
save(tse, file=paste0(outDir, "/", studyName, "_treeSE.rda"))

## make an MbioDataset, should TSS normalize and keep raw values as well by default
dataset <- MicrobiomeDB::importTreeSE(tse)

## get and prep picrust data
## TODO should either validate this exists, or make it optional if it doesnt
ecData <- data.table::fread(paste0(outDir, "/picrust/EC_pred_metagenome_unstrat_descrip.tsv"))
koData <- data.table::fread(paste0(outDir, "/picrust/KO_pred_metagenome_unstrat_descrip.tsv"))
metacycData <- data.table::fread(paste0(outDir, "/picrust/METACYC_path_abun_unstrat_descrip.tsv"))

# a helper
makeCollection <- function(dt, collectionName) {
    dt[, features := do.call(paste, c(.SD, sep = ":")), .SDcols = names(dt)[1:2]]
    dt[,1:2] <- NULL
    names(dt)[names(dt) == 'features'] <- collectionName

    recordIDs <- names(dt)[!names(dt) %in% collectionName]
    dt <- data.table::transpose(dt, make.names = collectionName)
    dt$recordIDs <- recordIDs
    recordIdColumn <- 'recordIDs'
    ancestorIdColumns <- character(0)

    collection <- veupathUtils::Collection(
        data = dt,
        recordIdColumn = recordIdColumn,
        ancestorIdColumns = ancestorIdColumns,
        name = collectionName
    )

    return(collection)
}

## make collections for picrust
ecCollection <- makeCollection(ecData, "EC Abundances")
koCollection <- makeCollection(koData, "Kegg Ontology Abundances")
metacycCollection <- makeCollection(metacycData, "METACYC Pathway Abundances")

## add collections to MbioDataset
numExistingCollections <- length(dataset@collections)
dataset@collections[[numExistingCollections + 1]] <- ecCollection
dataset@collections[[numExistingCollections + 2]] <- koCollection
dataset@collections[[numExistingCollections + 3]] <- metacycCollection

validObject(dataset)

## save MbioDataset as rda
save(dataset, file=paste0(outDir, "/", studyName, "_mbioDataset.rda"))
