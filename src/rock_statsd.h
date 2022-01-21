#ifndef __ROCK_STATSD_H
#define __ROCK_STATSD_H

extern char *statsd_config;
int is_valid_statsd_config(char *val, const char **err);

void init_statsd();
void send_metrics_to_statsd();

#endif
