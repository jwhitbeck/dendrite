#!/usr/bin/env Rscript

library(ggplot2)
library(grid)

args <- commandArgs(trailingOnly=TRUE)
res <- read.table(args[1], sep=',', header=TRUE)
outputFile <- args[2]

parallel_results <- res[grep('par|dendrite', res$name),];

reference <- 'edn-gz-par';

reference_read_time <- res[res$name==reference,c('avg_read_time')];
reference_file_size <- res[res$name==reference,c('file_size')];

svg(outputFile)

ggplot(parallel_results, aes(x=reference_read_time/avg_read_time, y=reference_file_size/file_size, label=name)) +
  geom_point(aes(colour=family), size=4) +
  geom_text(hjust=-0.1,vjust=-0.1,size=3,angle=45) +
  theme_bw() +
  theme(panel.border=element_rect(size=0),plot.margin=unit(c(0.5,0.5,1,1),'cm'),
        axis.title.x=element_text(vjust=-2), axis.title.y=element_text(vjust=2), legend.key=element_blank()) +
  scale_x_continuous(expand=c(0,0)) +
  scale_y_continuous(expand=c(0,0)) +
  expand_limits(x=0, y=0) +
  expand_limits(x=12, y=3) +
  labs(title="Full schema read times vs file size",
       y="File compression improvement (reference: gzipped EDN)",
       x="Read time speedup (reference: gzipped EDN)")

dev.off()
