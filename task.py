from datetime import datetime
from json import load
from os import system
from time import sleep

import pandas as pd
from requests import get

_DATA_DIR = 'data/'


class Requester:
    @staticmethod
    def make_request():
        r = get('https://www.predictit.org/api/marketdata/all/')
        open(_DATA_DIR + 'markets.json', 'wb').write(r.content)
        return r

    @property
    def _raw_markets(self):
        return load(open(_DATA_DIR + 'markets.json', encoding='utf8'))

    @property
    def _markets(self):
        return self._raw_markets['markets']


class Processor(Requester):
    def process(self):
        self._get_all_contract_data()
        self._create_dataframe()

    def _get_all_contract_data(self):
        self._data = []
        for market in self._markets:
            self._data.extend(self._get_contract_data(market, contract) for contract in market['contracts'])

    def _create_dataframe(self):
        self.arbs = pd.DataFrame(self._data).drop_duplicates()
        self.arbs.to_csv(_DATA_DIR + 'markets.csv', index=False)

    @staticmethod
    def _get_contract_data(market, contract):
        data = {}
        for market_field in ['shortName', 'url']:
            data[f'm{market_field}'] = market[market_field]
        for contract_field in ['name', 'bestBuyYesCost', 'bestBuyNoCost', 'bestSellYesCost', 'bestSellNoCost']:
            data[f'c{contract_field}'] = contract[contract_field]
        return data


class Calculator(Processor):
    _cost_cols = ['cbestBuyYesCost', 'cbestSellYesCost', 'cbestBuyNoCost', 'cbestSellNoCost']
    _revenue_and_profit_cols = ['contracts_ct', 'revenue', 'pi_cut']
    _market_cols = ['mshortName', 'murl']
    _contract_cols = ['cshortName', *_cost_cols]
    _initial_cols = _market_cols + _contract_cols
    _final_cols = [
        *_market_cols,
        *_cost_cols,
        *_revenue_and_profit_cols,
        'pi_cut_min',
        'pi_cut_less_min',
        'acct_fee',
        'profit_net',
    ]

    def calculate(self):
        self._calculate_at_contract_level()
        self._aggregate_at_market_level()
        self._calculate_at_market_level()
        self._filter_dataframe()
        if len(self.arbs):
            self._finalize_and_save_dataframe()

    def _calculate_at_contract_level(self):
        self.arbs = self.arbs[~self.arbs['cbestBuyNoCost'].isnull()].assign(contracts_ct=1).assign(revenue=1)
        self.arbs['pi_cut'] = self.arbs['cbestBuyNoCost'].apply(lambda x: (1 - x) * 0.1)
        self.arbs = self.arbs.assign(pi_cut_min=self.arbs['pi_cut'])

    def _aggregate_at_market_level(self):
        agg_dict = dict((sum_col, 'sum') for sum_col in self._cost_cols + self._revenue_and_profit_cols)
        agg_dict['pi_cut_min'] = 'min'
        self.arbs = self.arbs.groupby(by=self._market_cols, as_index=False, sort=False).agg(agg_dict)

    def _calculate_at_market_level(self):
        self.arbs['revenue'] = self.arbs['revenue'].apply(lambda x: x - 1)
        self.arbs['pi_cut_less_min'] = self.arbs['pi_cut'] - self.arbs['pi_cut_min']
        self.arbs['profit_net'] = self.arbs['revenue'] - self.arbs['cbestBuyNoCost'] - self.arbs['pi_cut_less_min']
        self.arbs['acct_fee'] = 0  # (self.arbs_agg['cbestBuyNoCost'] + self.arbs_agg['profit_cut']) * 0.05

    def _filter_dataframe(self):
        self.arbs = self.arbs[(self.arbs['contracts_ct'] > 1) & (self.arbs['profit_net'] > 0)].copy()

    def _finalize_and_save_dataframe(self):
        self.arbs = self.arbs.sort_values('profit_net', ascending=False)[self._final_cols]

        dttm = str(datetime.utcnow())
        log = pd.concat((self.arbs.assign(dttm=dttm), self._arbs_log))
        log.to_csv(self._arbs_log_fp, index=False)
        open(_DATA_DIR + 'summary.txt', 'w').write(self._calculate_profit_from_log(log=log))

    def _calculate_profit_from_log(self, log=None):
        if log is None:
            log = self._arbs_log.copy()
        min_profit_cutoff = 0
        log = log[log.profit_net >= min_profit_cutoff].drop_duplicates(subset=['murl'], keep='last')
        days_elapsed = (datetime.utcnow() - datetime(2021, 3, 29)).days + 1
        profit_net = log.profit_net.sum() * 850
        note = (
            f'Opportunities with minimum profit cutoff >= {min_profit_cutoff}',
            f'Since 3/29/21 - {days_elapsed} days: ${round(profit_net, 2)}',
            f'Monthly: ${round(profit_net * (30 / days_elapsed), 2)}',
            f'Annual: ${round(profit_net * (365 / days_elapsed), 2)}',
        )
        return '\n'.join(note)

    @property
    def _arbs_log(self):
        dtypes = {
            'mshortName': str,
            'murl': str,
            'cbestBuyYesCost': float,
            'cbestSellYesCost': float,
            'cbestBuyNoCost': float,
            'cbestSellNoCost': float,
            'contracts_ct': int,
            'revenue': int,
            'pi_cut': float,
            'pi_cut_min': float,
            'pi_cut_less_min': float,
            'acct_fee': float,
            'profit_net': float,
            'dttm': str,
        }
        return pd.read_csv(self._arbs_log_fp, usecols=dtypes.keys(), dtype=dtypes)

    @property
    def _arbs_log_fp(self):
        return _DATA_DIR + 'arbs.csv'


def _calculate():
    calculator = Calculator()
    response = calculator.make_request()
    if response.ok:
        calculator.process()
        calculator.calculate()
    return len(calculator.arbs)


def _run():
    if arbs_count := _calculate():
        commands = [
            'git config user.name "Actions on behalf of Devon Ankar"',
            'git config user.email "actions@users.noreply.github.com"',
            'git add -A',
            'git commit -m "Latest data: {0} ({1})" || exit 0'.format(datetime.utcnow().strftime(
                '%d %B %Y %H:%M'), arbs_count),
            'git push',
        ]
        for command in commands:
            system(command)


def main():
    start_time = datetime.utcnow()
    run_time = 6 * 60 * 60 - 150
    while True:
        if (datetime.utcnow() - start_time).total_seconds() >= run_time:
            break
        _run()
        sleep(60)


if __name__ == '__main__':
    main()
