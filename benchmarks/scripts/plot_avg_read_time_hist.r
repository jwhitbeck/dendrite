#!/usr/bin/env Rscript

library(ggplot2)

args <- commandArgs(trailingOnly=TRUE)
res <- read.table(args[1], sep=",", header=TRUE)
outputFile <- args[2]

svg(outputFile)

ggplot(res, aes(x=reorder(name,-avg_read_time))) +
  geom_bar(aes(y=avg_read_time,fill=family),stat='identity') +
  coord_flip() +
  theme_bw() +
  theme(panel.border=element_rect(size=0), axis.ticks.y=element_line(size=0), axis.title.y=element_blank()) +
  scale_y_continuous(expand=c(0,0)) +
  labs(title="Full schema read time (average over 10 runs, smaller is better)",y="Read time (s)")

dev.off()
