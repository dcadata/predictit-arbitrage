from datetime import datetime
from json import load, dump
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
        return load(open(_DATA_DIR + 'markets.json'))

    @property
    def _markets(self):
        return self._raw_markets['markets']


class Processor(Requester):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.data = []

    def process(self):
        self._filter_markets()
        self._create_df()

    def _filter_markets(self):
        for market in self._markets:
            self.data.extend(self._get_contract_data(market, contract) for contract in market['contracts'])

    def _create_df(self, save=None):
        self.arbs = pd.DataFrame(self.data).drop_duplicates()
        if save:
            self.arbs.to_csv(_DATA_DIR + 'markets.csv', index=False)

    @staticmethod
    def _get_contract_data(market, contract):
        market_fields = ['shortName', 'url']
        contract_fields = ['name', 'bestBuyYesCost', 'bestBuyNoCost', 'bestSellYesCost', 'bestSellNoCost']
        data = {}
        for market_field in market_fields:
            data[f'm{market_field}'] = market[market_field]
        for contract_field in contract_fields:
            data[f'c{contract_field}'] = contract[contract_field]
        return data


class Calculator(Processor):
    cost_cols = [
        'cbestBuyYesCost',
        'cbestSellYesCost',
        'cbestBuyNoCost',
        'cbestSellNoCost',
    ]
    revenue_and_profit_cols = [
        'contracts_ct',
        'revenue',
        'pi_cut',
    ]
    market_cols = [
        'mshortName',
        'murl',
    ]
    contract_cols = [
        'cshortName',
        *cost_cols,
    ]

    initial_cols = market_cols + contract_cols

    final_cols = [
        *market_cols,

        *cost_cols,
        *revenue_and_profit_cols,
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
        self.arbs = self.arbs[~self.arbs['cbestBuyNoCost'].isnull()].copy()
        self.arbs = self.arbs.assign(contracts_ct=1).assign(revenue=1)
        self.arbs['pi_cut'] = self.arbs['cbestBuyNoCost'].apply(lambda x: (1 - x) * 0.1)
        self.arbs = self.arbs.assign(pi_cut_min=self.arbs['pi_cut'])

    def _aggregate_at_market_level(self):
        agg_dict = {}
        for sum_col in self.cost_cols + self.revenue_and_profit_cols:
            agg_dict.update({sum_col: 'sum'})
        agg_dict.update({'pi_cut_min': 'min'})

        self.arbs = self.arbs.groupby(by=self.market_cols, as_index=False, sort=False).agg(agg_dict)

    def _calculate_at_market_level(self):
        self.arbs['revenue'] = self.arbs['revenue'].apply(lambda x: x - 1)
        self.arbs['pi_cut_less_min'] = self.arbs['pi_cut'] - self.arbs['pi_cut_min']
        self.arbs['profit_net'] = (
                self.arbs['revenue'] - self.arbs['cbestBuyNoCost'] - self.arbs['pi_cut_less_min'])
        self.arbs['acct_fee'] = 0  # (self.arbs_agg['cbestBuyNoCost'] + self.arbs_agg['profit_cut']) * 0.05

    def _filter_dataframe(self):
        self.arbs = self.arbs[(self.arbs['contracts_ct'] > 1) & (self.arbs['profit_net'] > 0)].copy()

    def _finalize_and_save_dataframe(self):
        self.arbs = self.arbs.sort_values('profit_net', ascending=False)
        self.arbs = self.arbs[self.final_cols]

        dttm = str(datetime.today())
        log = pd.concat((self.arbs.assign(dttm=dttm), self._arbs_log))
        log.to_csv(self._arbs_log_fp, index=False)
        open(_DATA_DIR + 'profit_net.txt', 'w').write(self._calculate_profit_from_log(log=log))
        dump(self._raw_markets, open(_DATA_DIR + f'markets/{dttm}.json', 'w'))

    def _calculate_profit_from_log(self, log=None):
        if log is None:
            log = self._arbs_log.copy()
        log = log[log.profit_net >= 10 / 850]
        days_elapsed = (datetime.today() - datetime(2021, 3, 29)).days + 1
        profit_net = log.profit_net.sum() * 850
        note = (
            f'Since 3/29/21 - {days_elapsed} days: ${round(profit_net, 2)}',
            f'Monthly: ${round(profit_net * (30 / days_elapsed), 2)}',
            f'Annual: ${round(profit_net * (365 / days_elapsed), 2)}',
        )
        return '\n'.join(note)

    @property
    def _arbs_log(self):
        return pd.read_csv(self._arbs_log_fp)

    @property
    def _arbs_log_fp(self):
        return _DATA_DIR + 'arbs.csv'


def main():
    calc = Calculator()
    r = calc.make_request()
    if r.ok:
        calc.process()
        calc.calculate()


if __name__ == '__main__':
    main()
