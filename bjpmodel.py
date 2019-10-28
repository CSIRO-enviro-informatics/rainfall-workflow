import os, sys, shutil
from datetime import datetime

import numpy as np

import pytrans
import pybjp


class BjpModel:

    OBSERVED_DATA_CODE = 1
    CENSORED_DATA_CODE = 2
    MISSING_DATA_CODE = 3
    MISSING_DATA_VALUE = -9999.0

    FIXED_RANDOM_SEED = 5

    NO_CENS_THRESH = -99999.0
    ZERO_CENS_THRESH = 0.0
    GAUGE_CENS_THRESH = 0.2


    def __init__(self, num_vars, groups, burn=3000, chainlength=7000, seed='random'):

        self.num_vars = num_vars
        self.burn = burn
        self.chainlength = chainlength

        assert (seed == 'random' or seed == 'fixed')
        if seed == 'fixed':
            np.random.seed(self.FIXED_RANDOM_SEED)
            self.seed = self.FIXED_RANDOM_SEED
        else:
            self.seed = np.random.randint(0,100000)

        self.groups = groups

        self.censors = []
        for i in range(num_vars):
            if groups[i] == 10 or groups[i] == 20:
                censor = self.ZERO_CENS_THRESH
            elif groups[i] == 30 or groups[i] == 100:
                censor = self.NO_CENS_THRESH
            elif groups[i] == 50:
                censor = self.GAUGE_CENS_THRESH

            self.censors.append(censor)

        self.censors = np.array(self.censors)


        self.bjp_wrapper = None
        self.bjp_fitting_data = None


    def prepare_bjp_data(self, fit_data, group, censor, trformer=None):

        fit_data = np.array(fit_data, copy=True)

        # Set the censored and missing data flags and adjust data accordingly
        flags = np.ones(fit_data.shape, dtype='intc', order='C')*self.OBSERVED_DATA_CODE

        censor_idx = fit_data <= censor
        flags[censor_idx] = self.CENSORED_DATA_CODE

        # Treat both -9999.0 and np.nan as missing values in the input data
        missing_idx = np.abs(fit_data - self.MISSING_DATA_VALUE) < 1E-6
        flags[missing_idx] = self.MISSING_DATA_CODE

        missing_idx2 = np.isnan(fit_data)
        flags[missing_idx2] = self.MISSING_DATA_CODE

        # set NaNs to missing data value
        fit_data[np.isnan(fit_data)] = self.MISSING_DATA_VALUE

        if trformer is None:

            # remove missing values before estimating transformation parameters
            fit_data_for_trans = np.array(fit_data, copy=True)
            fit_data_for_trans = fit_data_for_trans[flags != self.MISSING_DATA_CODE]

            if group == 10 or group == 50:
                print(np.nanmax(fit_data_for_trans))
                trformer = pytrans.PyLogSinh(scale=5.0/np.max(fit_data_for_trans))
                trformer.optim_params(fit_data_for_trans, censor, do_rescale=True, is_map=True)
            elif group == 20 or group == 30:
                trformer = pytrans.PyYJT(scale=1.0/np.std(fit_data_for_trans), shift=-1.0*np.mean(fit_data_for_trans))
                trformer.optim_params(fit_data_for_trans, censor, do_rescale=True, is_map=True)
            else:
                print('Transformation group code not recognised. Exiting.')
                sys.exit()

        fit_data[fit_data < censor] = censor

        rs_data = trformer.rescale_many(fit_data)
        tr_data = trformer.transform_many(rs_data)

        rs_censor = trformer.rescale_one(censor)
        tr_censor = trformer.transform_one(rs_censor)

        # restore some dummy values for missing data
        fit_data[np.isnan(fit_data)] = self.MISSING_DATA_VALUE

        bjp_data = {}
        bjp_data['trformer'] = trformer
        bjp_data['tr_data'] = tr_data
        bjp_data['censor'] = censor
        bjp_data['tr_censor'] = tr_censor
        bjp_data['flags'] = flags

        return bjp_data



    def prepare_fc_data(self, predictor_values, transformers):


        assert len(transformers) == len(self.censors)

        if np.any(np.isnan(predictor_values)) or np.any(predictor_values == self.MISSING_DATA_VALUE):
            print("Warning: Predictor is NaN or missing")
            predictor_values[np.isnan(predictor_values)] = -9999.0

        # should we limit extreme new predictor values ???
        bjp_data_new = {}
        bjp_data_new['tr_data'] = np.array([self.MISSING_DATA_VALUE]*self.num_vars)
        bjp_data_new['flags'] = np.array([self.MISSING_DATA_CODE]*self.num_vars, dtype='intc')

        bjp_data_new['censor'] = np.array([self.censors[i] for i in range(self.num_vars)])
        bjp_data_new['tr_censor'] = np.array([transformers[i].transform_one(transformers[i].rescale_one(self.censors[i])) for i in range(self.num_vars)])


        for i in range(len(predictor_values)):

            if np.abs(predictor_values[i] - self.MISSING_DATA_VALUE) < 1E-6:
                bjp_data_new['flags'][i] = self.MISSING_DATA_CODE
            elif predictor_values[i] <= bjp_data_new['censor'][i]:
                bjp_data_new['flags'][i] = self.CENSORED_DATA_CODE
                predictor_values[i] = self.censors[i]
            else:
                bjp_data_new['flags'][i] = self.OBSERVED_DATA_CODE

            trformer = transformers[i]
            bjp_data_new['trformer'] = trformer

            rs_pred = trformer.rescale_one(predictor_values[i])
            tr_pred = trformer.transform_one(rs_pred)

            bjp_data_new['tr_data'][i] = tr_pred


        return bjp_data_new



    def join_ptor_ptand_data(self, bjp_fitting_data):

        joined_data = {'trformer': [], 'tr_data': [], 'censor': [], 'tr_censor': [], 'flags':[]}

        for i in range(len(bjp_fitting_data)):

            joined_data['trformer'].append(bjp_fitting_data[i]['trformer'])
            joined_data['tr_data'].append(bjp_fitting_data[i]['tr_data'])
            joined_data['censor'].append(bjp_fitting_data[i]['censor'])
            joined_data['tr_censor'].append(bjp_fitting_data[i]['tr_censor'])
            joined_data['flags'].append(bjp_fitting_data[i]['flags'])

        joined_data['tr_data'] = np.array(joined_data['tr_data'], order='C')
        joined_data['censor'] = np.array(joined_data['censor'], order='C')
        joined_data['tr_censor'] = np.array(joined_data['tr_censor'], order='C')
        joined_data['flags'] = np.array(joined_data['flags'], order='C')

        return joined_data



    def inv_transform(self, data, trformer):

        inv_tr_data = trformer.inv_transform_many(data)
        inv_rs_data = trformer.inv_rescale_many(inv_tr_data)

        return inv_rs_data



    def sample(self, obs):

        # obs has dimensions num_vars x num_time_periods

        bjp_fitting_data = []

        for i in range(self.num_vars):

            bjp_fitting_data.append(self.prepare_bjp_data(obs[i], self.groups[i], self.censors[i]))

        bjp_fitting_data = self.join_ptor_ptand_data(bjp_fitting_data)

        bjp_wrapper = pybjp.PyBJP(self.num_vars, self.burn, self.chainlength, self.seed)

        mu, cov = bjp_wrapper.sample(bjp_fitting_data['tr_data'], bjp_fitting_data['flags'], bjp_fitting_data['tr_censor'])

        self.bjp_wrapper = bjp_wrapper
        self.bjp_fitting_data = bjp_fitting_data

        tparams = []
        for trformer in bjp_fitting_data['trformer']:
            tparams.append(trformer.get_params())
        tparams = np.array(tparams)

        return mu, cov, tparams


    def forecast(self, predictor_values, transformers, mu, cov, gen_climatology=False, convert_cens=True):

        self.bjp_wrapper = pybjp.PyBJP(self.num_vars, self.burn, self.chainlength, self.seed)
        bjp_fc_data = self.prepare_fc_data(predictor_values, transformers)
        print(bjp_fc_data['tr_data'].dtype, bjp_fc_data['flags'].dtype, bjp_fc_data['tr_censor'].dtype, mu.dtype, cov.dtype, (cov.shape[0] / 2))
        forecasts = self.bjp_wrapper.forecast2(bjp_fc_data['tr_data'], bjp_fc_data['flags'], bjp_fc_data['tr_censor'], mu.astype(np.float64), cov.astype(np.float64), int(cov.shape[0] / 2))

        for i in range(self.num_vars):
            
            trformer = transformers[i]

            forecasts[:, i] = self.inv_transform(forecasts[:, i], trformer)

            if convert_cens:
                cens = self.censors[i]
                forecasts[:, i][forecasts[:, i] < cens] = cens

        res = {}

        res['forecast'] = forecasts

        if gen_climatology:

            bjp_clims = self.bjp_wrapper.gen_climatology()

            clim = np.empty(forecasts.shape)*np.nan

            for i in range(self.num_vars):
                trformer = self.bjp_fitting_data['trformer'][i]
                clim[:,i] = self.inv_transform(bjp_clims[:, i], trformer)

                if convert_cens:
                    cens = bjp_fc_data['censor'][i]
                    clim[:, i][clim[:, i] < cens] = cens

            res['clim'] = clim



        return res
