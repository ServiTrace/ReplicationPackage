# %% Imports
import pandas as pd
from plotnine import *
import numpy as np
from sb_importer import *

# %%
raw_df = pd.read_csv('../data/lg12_exp3_todo_api/load_level_validation.csv')

# %%
fluctuating_df = raw_df.loc[raw_df['iteration'] == 'exp3_workload_types_20min_fluctuating-4-fluctuating', :].copy()

# %%
simplescaled = fluctuating_df.loc[fluctuating_df['variable'] == 'workload_rates_ips', 'invocations_per_second']*15
withss_df = fluctuating_df.copy()
withss_df.loc[fluctuating_df['variable'] == 'workload_rates_ips', 'invocations_per_second'] = simplescaled

# %%
generated_workload_df = fluctuating_df.loc[fluctuating_df['variable'] == 'workload_options_ips', :].reset_index(drop=True)
generated_workload_invocations = generated_workload_df['invocations_per_second']
invoked_df = fluctuating_df.loc[(fluctuating_df['variable'] == 'k6_invocations_ips'), :].reset_index(drop=True)
traced_df = fluctuating_df.loc[(fluctuating_df['variable'] == 'trace_breakdown_ips'), :].reset_index(drop=True)
invoked_norm_df = invoked_df.copy()
traced_norm_df = traced_df.copy()

sideplot = pd.concat([generated_workload_df, invoked_df, traced_df])
sideplot['variable'] = sideplot['variable'].str.replace('k6_invocations_ips', 'Sent').replace('trace_breakdown_ips', 'Executed').replace('workload_options_ips', 'Planned')
sideplot['variable'] = pd.Categorical(sideplot['variable'], categories=['Planned', 'Sent', 'Executed', 'Ratio'], ordered=True)

invoked_norm_df['invocations_per_second'] = invoked_df['invocations_per_second'] / generated_workload_invocations
traced_norm_df['invocations_per_second'] = traced_df['invocations_per_second'] / generated_workload_invocations
to_plot = pd.concat([invoked_norm_df, traced_norm_df])
to_plot['variable'] = to_plot['variable'].str.replace('k6_invocations_ips', 'Sent vs. Planned').replace('trace_breakdown_ips', 'Executed vs. Planned')

# %%
plt = ggplot(to_plot.loc[to_plot['relative_time'] > 0, :]) +\
    theme_light(base_size=12) +\
    theme(figure_size=(4, 2)) +\
    geom_line(aes(x='relative_time', y='invocations_per_second', group='variable')) +\
    facet_wrap('variable', nrow=2) +\
    xlab('Time [s]') + ylab('Ratio')

plt.save(path=plots_path(), filename='trace_vs_workload.pdf')
plt

# %%
plt = ggplot(sideplot.loc[(sideplot['relative_time'] > 0) & (sideplot['relative_time'] < 50), :]) +\
    theme_light(base_size=12) +\
    theme(figure_size=(1.5, 2.1), legend_position='right',
         legend_background=element_rect(alpha=0), legend_margin=0,
         legend_box_margin=0) +\
    geom_line(aes(x='relative_time', y='invocations_per_second', group='variable', color='variable')) +\
    xlab('Time [s]') + ylab('Reqs per Second') +\
    guides(color=guide_legend(nrow=4, title='')) +\
    scale_color_manual(values={
        'Planned': 'magenta',
        'Sent': 'orangered',
        'Executed': 'deepskyblue',
        'Ratio': 'Black'
    })

plt.save(path=plots_path(), filename='trace_vs_workload_sideplot.pdf')
plt