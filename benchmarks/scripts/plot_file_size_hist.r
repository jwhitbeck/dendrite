#!/usr/bin/env Rscript

library(ggplot2)

args <- commandArgs(trailingOnly=TRUE)
res <- read.table(args[1], sep=",", header=TRUE)
outputFile <- args[2]

svg(outputFile)

ggplot(res, aes(x=reorder(name,-file_size))) +
  geom_bar(aes(y=(file_size/(1024*1024)),fill=family),stat='identity') +
  coord_flip() +
  theme_bw() +
  theme(panel.border=element_rect(size=0), axis.ticks.y=element_line(size=0), axis.title.y=element_blank()) +
  scale_y_continuous(expand=c(0,0)) +
  labs(title="File size (smaller is better)",y="File size (MB)")

dev.off()
