import os
from datetime import datetime
from time import sleep

import pandas as pd
import requests

_DATA_DIR = 'data/'
_ARBS_FP = _DATA_DIR + 'arbs.csv'
_ACTIONABLE_ARBS_FP = _DATA_DIR + 'actionable_arbs.csv'
_SUMMARY_FP = _DATA_DIR + 'summary.txt'


class Calculator:
    def __init__(self):
        self._cost_cols = ['cbestBuyYesCost', 'cbestSellYesCost', 'cbestBuyNoCost', 'cbestSellNoCost']
        self._revenue_and_profit_cols = ['contracts_ct', 'revenue', 'pi_cut']
        self._market_cols = ['mshortName', 'murl']
        self._contract_cols = ['cshortName', *self._cost_cols]
        self._initial_cols = self._market_cols + self._contract_cols
        self._final_cols = [
            *self._market_cols, *self._cost_cols, *self._revenue_and_profit_cols,
            'pi_cut_min', 'pi_cut_less_min', 'profit_net',
        ]

    def calculate(self) -> None:
        self._make_request()
        if not self._markets_response.ok:
            return
        self._get_contract_data()
        self._calculate_at_contract_level()
        self._aggregate_at_market_level()
        self._calculate_at_market_level()
        self._filter_data()
        if not len(self.arbs):
            return
        self._update_and_save_arbs()
        self._filter_on_actionable_arbs()
        self._create_text_summary()

    def _make_request(self) -> None:
        self._markets_response = requests.get('https://www.predictit.org/api/marketdata/all/')

    def _get_contract_data(self) -> None:
        arbs_data = []
        for market in self._markets_response.json()['markets']:
            arbs_data.extend(_get_contract_data(market, contract) for contract in market['contracts'])
        self.arbs = pd.DataFrame(arbs_data).drop_duplicates()

    def _calculate_at_contract_level(self) -> None:
        self.arbs = self.arbs[~self.arbs['cbestBuyNoCost'].isnull()].assign(contracts_ct=1).assign(revenue=1)
        self.arbs['pi_cut'] = self.arbs['cbestBuyNoCost'].apply(lambda x: (1 - x) * 0.1)
        self.arbs = self.arbs.assign(pi_cut_min=self.arbs['pi_cut'])

    def _aggregate_at_market_level(self) -> None:
        agg_dict = dict((sum_col, 'sum') for sum_col in self._cost_cols + self._revenue_and_profit_cols)
        agg_dict['pi_cut_min'] = 'min'
        self.arbs = self.arbs.groupby(by=self._market_cols, as_index=False, sort=False).agg(agg_dict)

    def _calculate_at_market_level(self) -> None:
        self.arbs['revenue'] = self.arbs['revenue'].apply(lambda x: x - 1)
        self.arbs['pi_cut_less_min'] = self.arbs['pi_cut'] - self.arbs['pi_cut_min']
        self.arbs['profit_net'] = self.arbs['revenue'] - self.arbs['cbestBuyNoCost'] - self.arbs['pi_cut_less_min']

    def _filter_data(self) -> None:
        self.arbs = self.arbs[(self.arbs['contracts_ct'] > 1) & (self.arbs['profit_net'] > 0)].copy()

    def _update_and_save_arbs(self) -> None:
        self.arbs = self.arbs.sort_values('profit_net', ascending=False)[self._final_cols]
        pd.concat((self.arbs.assign(dttm=str(datetime.utcnow())), self._arbs_log)).to_csv(_ARBS_FP, index=False)

    def _filter_on_actionable_arbs(self) -> None:
        arbs_log = self._arbs_log.copy()
        self._actionable_arbs = arbs_log[arbs_log.profit_net > 0].drop_duplicates(subset=['murl'], keep='last')
        self._actionable_arbs.to_csv(_ACTIONABLE_ARBS_FP, index=False)

    def _create_text_summary(self) -> None:
        profit_net = self._actionable_arbs.profit_net.sum() * 850
        days_elapsed = (datetime.utcnow() - datetime(2021, 3, 29)).days + 1
        summary = '  \n'.join((
            'Opportunities with profit:',
            f'Since 3/29/21 - {days_elapsed} days: ${round(profit_net, 2):,}',
            f'Monthly: ${round(profit_net * (30 / days_elapsed), 2):,}',
            f'Annual: ${round(profit_net * (365 / days_elapsed), 2):,}',
        ))
        open(_SUMMARY_FP, 'w').write(summary)
        readme = open('README.md').read().split('\n\n---\n\n', 1)[0]
        open('README.md', 'w').write('\n\n'.join((readme, '---', '## Summary', summary)))

    @property
    def _arbs_log(self) -> pd.DataFrame:
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
            'profit_net': float,
            'dttm': str,
        }
        return pd.read_csv(_ARBS_FP, usecols=dtypes.keys(), dtype=dtypes)


def _get_contract_data(market: dict, contract: dict) -> dict:
    data = {}
    for market_field in ('shortName', 'url'):
        data[f'm{market_field}'] = market[market_field]
    for contract_field in ('name', 'bestBuyYesCost', 'bestBuyNoCost', 'bestSellYesCost', 'bestSellNoCost'):
        data[f'c{contract_field}'] = contract[contract_field]
    return data


def run() -> None:
    calculator = Calculator()
    calculator.calculate()
    if arbs_count := len(calculator.arbs):
        for command in (
                'git config user.name "Automated"',
                'git config user.email "actions@users.noreply.github.com"',
                'git add -A',
                'git commit -m "Latest data: {0} ({1})" || exit 0'.format(datetime.utcnow().strftime(
                    '%d %B %Y %H:%M'), arbs_count),
                'git push',
        ):
            os.system(command)


def main():
    start_time = datetime.utcnow()
    run_time = 6 * 60 * 60 - 150
    while True:
        if (datetime.utcnow() - start_time).total_seconds() >= run_time:
            break
        run()
        sleep(60)


if __name__ == '__main__':
    main()
