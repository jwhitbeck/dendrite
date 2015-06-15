#!/usr/bin/env Rscript

library(ggplot2)
library(grid)

args <- commandArgs(trailingOnly=TRUE)
res <- read.table(args[1], sep=',', header=TRUE)
outputFile <- args[2]

fit <- lm(res$query_time ~ res$max_column_length + res$num_columns)

intercept <- fit$coefficients[1]
max_column_length_slope <- fit$coefficients[[2]]

svg(outputFile)

ggplot(res, aes(x=max_column_length/(1024*1024), y=query_time, colour=num_columns)) +
  geom_point() +
  geom_abline(intercept=intercept, slope=max_column_length_slope*1024*1024) +
  theme_bw() +
  theme(panel.border=element_rect(size=0),plot.margin=unit(c(0.5,0.5,1,1),'cm'),
        axis.title.x=element_text(vjust=-2), axis.title.y=element_text(vjust=2)) +
  scale_x_continuous(expand=c(0,0)) +
  scale_y_continuous(expand=c(0,0)) +
  expand_limits(x=0, y=0) +
  labs(title="Query speed (multilinear fit)",
       y="Read time (ms)",
       x="Max column size (MB)",
       colour="# columns")

dev.off()
