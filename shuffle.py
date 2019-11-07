import os, sys
from subprocess import call
from datetime import date, datetime

import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta

import shuffle

first_year = 1970
last_year = 2015


def shuffle(ens, obs):

    ens = np.array(ens)
    obs = np.array(obs)

    assert len(ens) == len(obs)
    obs_ranks = obs.argsort().argsort()
    shuffled_ens = np.sort(ens)[obs_ranks]

    return shuffled_ens



def shuffle_random_ties(ens, obs):
    # For use with ties

    ens = np.array(ens)
    obs = np.array(obs)

    assert len(ens)==len(obs)

    obs_ranks = obs.argsort().argsort()

    unique, counts = np.unique(ens, return_counts=True)
    ties = unique[np.where(counts > 1)]

    for t in ties:

        orig_idx = np.where(obs==t)[0]
        new_idx = np.random.permutation(orig_idx)

        obs_ranks[orig_idx] = obs_ranks[new_idx]

    shuffled_ens = np.sort(ens)[obs_ranks]

    return shuffled_ens


def shuffle_random_ties_block(ens, obs):
    # For use with ties

    ens = np.array(ens)
    obs = np.array(obs)

    forecast_chunked = (zip(*[iter(ens)] * len(obs)))

    shuffled_ens = []

    chunks = []
    for i, chunk in enumerate(forecast_chunked):
        chunks.append(chunk)
        shuffled_ens.append(shuffle_random_ties(chunk, obs))

    shuffled_ens = np.array(shuffled_ens).flatten()

    n1 = len(shuffled_ens)
    n2 = (len(ens)-n1)

    shuffled_ens2 = shuffle_random_ties(ens[n1:], np.random.permutation(obs[:n2]))

    shuffled_ens = np.concatenate([shuffled_ens, shuffled_ens2])



    return shuffled_ens



def get_shuffle_dates(fc_init_month, fc_init_day, days_offsets, num_sequences, num_steps, xv_year,
                      years_range=None, step='day'):

    assert step in ['day', 'month']
    if step == 'month':
        assert fc_init_day == 1

    if years_range is None:
        years_range = range(first_year, last_year+1)

    shk_shuff_dates = []

    for offset in days_offsets:

        for yr in years_range:

            # need to exclude previous year as well since it is a 12 month forecast - can't use the last month
            if yr != xv_year and yr != xv_year-1:

                orig_date = datetime(yr, fc_init_month, fc_init_day)
                start_date = datetime(yr, fc_init_month, fc_init_day) + relativedelta(days=offset)

                if step == 'day':
                    tgt_dates = [start_date + relativedelta(days=i) for i in range(num_steps)]
                elif step == 'month':
                    days_per_month = [(orig_date + relativedelta(months=i+1, days=-1)).day for i in range(num_steps)]
                    tgt_dates = [start_date]
                    for i in range(num_steps):
                        tgt_dates.append(tgt_dates[-1] + relativedelta(days=days_per_month[i]))

                shk_shuff_dates.append(tgt_dates)


    shk_shuff_dates = np.array(shk_shuff_dates)

    sel_idx = np.random.permutation(np.arange(len(shk_shuff_dates)))[:num_sequences]
    shk_shuff_dates = shk_shuff_dates[sel_idx, :]

    try:
        assert len(shk_shuff_dates) == num_sequences
    except AssertionError:
        print('Couldn\'t obtain the requested number of sequences. Try adding more offsets or using more years')


    return shk_shuff_dates

#n_ens = vrf_scores.n_ens[model]

# n_ens = 100
#
# fc_init_year=1980
# fc_init_month = 9
# fc_init_day = 1
# shk_shuff_dates, shk_shuff_offsets = get_shuffle_dates(fc_init_month, fc_init_day, [-5, 0], 10, 3, xv_year=fc_init_year, step='month')
#
#
# for e_i, e in enumerate(shk_shuff_dates):
#     print(e, shk_shuff_offsets[e_i] )
#
# print(shk_shuff_dates.shape)