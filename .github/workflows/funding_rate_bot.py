#!/usr/bin/env python3
"""
🎯 Funding Rate Arbitrage Bot
============================

Bot automatique pour détecter les opportunités d'arbitrage de funding rates
entre exchanges crypto (Binance, Bybit, OKX, Bitget, KuCoin).

Author: Marc R
License: MIT
Version: 1.0.0
"""

import requests
import pandas as pd
import time
import logging
from datetime import datetime, timedelta
import json
from typing import Dict, List, Optional, Tuple
import numpy as np
from dataclasses import dataclass
import schedule
import os
import sys

# Import de la configuration
try:
    from config import config
except ImportError:
    # Configuration par défaut si config.py n'existe pas
    class DefaultConfig:
        SYMBOLS = ['BTC', 'ETH', 'SOL', 'ADA', 'MATIC', 'DOT', 'AVAX']
        MIN_PROFIT_THRESHOLD = 0.005
        POSITION_SIZE = 1000
        SCAN_INTERVAL = 30
        ENABLED_EXCHANGES = ['binance', 'bybit', 'okx', 'bitget', 'kucoin']
        EXCHANGE_FEES = {
            'Binance': 0.08, 'Bybit': 0.08, 'OKX': 0.09, 
            'Bitget': 0.10, 'KuCoin': 0.09
        }
        SLIPPAGE_ESTIMATES = {
            'BTC': 0.01, 'ETH': 0.02, 'SOL': 0.03, 'ADA': 0.04,
            'MATIC': 0.04, 'DOT': 0.03, 'AVAX': 0.03
        }
        LOG_LEVEL = 'INFO'
        LOG_FILE = 'funding_rate_bot.log'
        DATA_DIR = 'data'
    
    config = DefaultConfig()

# Configuration du logging
os.makedirs(config.DATA_DIR, exist_ok=True)
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler()
    ]
)

@dataclass
class FundingRateData:
    """Structure pour les données de funding rate"""
    exchange: str
    symbol: str
    rate: float
    timestamp: datetime
    next_funding_time: Optional[datetime] = None
    mark_price: Optional[float] = None

@dataclass
class ArbitrageOpportunity:
    """Structure pour les opportunités d'arbitrage"""
    long_exchange: str
    short_exchange: str
    symbol: str
    long_rate: float
    short_rate: float
    rate_difference: float
    potential_profit_8h: float
    estimated_fees: float
    net_profit_8h: float
    next_funding_time: Optional[datetime] = None

class ExchangeAPI:
    """Classe de base pour les APIs des exchanges"""
    
    def __init__(self, base_url: str, name: str):
        self.base_url = base_url
        self.name = name
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.last_request_time = 0
        self.min_request_interval = 0.5  # 500ms entre requêtes
    
    def _rate_limit(self):
        """Applique le rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Fait une requête avec gestion d'erreurs"""
        self._rate_limit()
        
        try:
            url = f"{self.base_url}/{endpoint}"
            response = self.session.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                return response.json()
            else:
                logging.warning(f"{self.name} API error: {response.status_code}")
                return None
                
        except Exception as e:
            logging.error(f"{self.name} request error: {e}")
            return None

class BinanceAPI(ExchangeAPI):
    """API Binance Futures"""
    
    def __init__(self):
        super().__init__("https://fapi.binance.com", "Binance")
        self.symbol_mapping = {
            'BTC': 'BTCUSDT', 'ETH': 'ETHUSDT', 'SOL': 'SOLUSDT',
            'ADA': 'ADAUSDT', 'MATIC': 'MATICUSDT', 'DOT': 'DOTUSDT', 'AVAX': 'AVAXUSDT'
        }
    
    def get_funding_rates(self, symbols: List[str]) -> List[FundingRateData]:
        """Récupère les funding rates de Binance"""
        funding_data = []
        
        try:
            data = self._make_request("fapi/v1/premiumIndex")
            if not data:
                return funding_data
            
            for item in data:
                symbol_name = item.get('symbol', '')
                base_symbol = None
                
                for base, full in self.symbol_mapping.items():
                    if symbol_name == full:
                        base_symbol = base
                        break
                
                if base_symbol and base_symbol in symbols:
                    funding_rate = float(item.get('lastFundingRate', 0))
                    next_funding_time = None
                    
                    if 'nextFundingTime' in item:
                        next_funding_time = datetime.fromtimestamp(
                            int(item['nextFundingTime']) / 1000
                        )
                    
                    funding_data.append(FundingRateData(
                        exchange="Binance",
                        symbol=base_symbol,
                        rate=funding_rate,
                        timestamp=datetime.now(),
                        next_funding_time=next_funding_time,
                        mark_price=float(item.get('markPrice', 0))
                    ))
                    
        except Exception as e:
            logging.error(f"Binance funding rates error: {e}")
        
        return funding_data

class BybitAPI(ExchangeAPI):
    """API Bybit"""
    
    def __init__(self):
        super().__init__("https://api.bybit.com", "Bybit")
        self.symbol_mapping = {
            'BTC': 'BTCUSDT', 'ETH': 'ETHUSDT', 'SOL': 'SOLUSDT',
            'ADA': 'ADAUSDT', 'MATIC': 'MATICUSDT', 'DOT': 'DOTUSDT', 'AVAX': 'AVAXUSDT'
        }
    
    def get_funding_rates(self, symbols: List[str]) -> List[FundingRateData]:
        """Récupère les funding rates de Bybit"""
        funding_data = []
        
        try:
            data = self._make_request("v5/market/instruments-info", {'category': 'linear'})
            if not data or 'result' not in data:
                return funding_data
            
            instruments = data['result'].get('list', [])
            
            for instrument in instruments:
                symbol_name = instrument.get('symbol', '')
                base_symbol = None
                
                for base, full in self.symbol_mapping.items():
                    if symbol_name == full:
                        base_symbol = base
                        break
                
                if base_symbol and base_symbol in symbols:
                    funding_rate = float(instrument.get('fundingRate', 0))
                    next_funding_time = None
                    
                    if 'nextFundingTime' in instrument:
                        next_funding_time = datetime.fromtimestamp(
                            int(instrument['nextFundingTime']) / 1000
                        )
                    
                    funding_data.append(FundingRateData(
                        exchange="Bybit",
                        symbol=base_symbol,
                        rate=funding_rate,
                        timestamp=datetime.now(),
                        next_funding_time=next_funding_time,
                        mark_price=float(instrument.get('markPrice', 0))
                    ))
                    
        except Exception as e:
            logging.error(f"Bybit funding rates error: {e}")
        
        return funding_data

class OKXAPI(ExchangeAPI):
    """API OKX"""
    
    def __init__(self):
        super().__init__("https://www.okx.com", "OKX")
        self.symbol_mapping = {
            'BTC': 'BTC-USDT-SWAP', 'ETH': 'ETH-USDT-SWAP', 'SOL': 'SOL-USDT-SWAP',
            'ADA': 'ADA-USDT-SWAP', 'MATIC': 'MATIC-USDT-SWAP', 'DOT': 'DOT-USDT-SWAP', 'AVAX': 'AVAX-USDT-SWAP'
        }
    
    def get_funding_rates(self, symbols: List[str]) -> List[FundingRateData]:
        """Récupère les funding rates d'OKX"""
        funding_data = []
        
        for symbol in symbols:
            try:
                if symbol not in self.symbol_mapping:
                    continue
                
                okx_symbol = self.symbol_mapping[symbol]
                data = self._make_request("api/v5/public/funding-rate", {'instId': okx_symbol})
                
                if data and 'data' in data and data['data']:
                    item = data['data'][0]
                    funding_rate = float(item.get('fundingRate', 0))
                    next_funding_time = None
                    
                    if 'nextFundingTime' in item:
                        next_funding_time = datetime.fromtimestamp(
                            int(item['nextFundingTime']) / 1000
                        )
                    
                    funding_data.append(FundingRateData(
                        exchange="OKX",
                        symbol=symbol,
                        rate=funding_rate,
                        timestamp=datetime.now(),
                        next_funding_time=next_funding_time
                    ))
                
                time.sleep(0.1)
                    
            except Exception as e:
                logging.error(f"OKX funding rate error for {symbol}: {e}")
                continue
        
        return funding_data

class BitgetAPI(ExchangeAPI):
    """API Bitget"""
    
    def __init__(self):
        super().__init__("https://api.bitget.com", "Bitget")
        self.symbol_mapping = {
            'BTC': 'BTCUSDT_UMCBL', 'ETH': 'ETHUSDT_UMCBL', 'SOL': 'SOLUSDT_UMCBL',
            'ADA': 'ADAUSDT_UMCBL', 'MATIC': 'MATICUSDT_UMCBL', 'DOT': 'DOTUSDT_UMCBL', 'AVAX': 'AVAXUSDT_UMCBL'
        }
    
    def get_funding_rates(self, symbols: List[str]) -> List[FundingRateData]:
        """Récupère les funding rates de Bitget"""
        funding_data = []
        
        try:
            data = self._make_request("api/mix/v1/market/contracts", {'productType': 'umcbl'})
            if not data or 'data' not in data:
                return funding_data
            
            for item in data['data']:
                symbol_name = item.get('symbol', '')
                base_symbol = None
                
                for base, full in self.symbol_mapping.items():
                    if symbol_name == full:
                        base_symbol = base
                        break
                
                if base_symbol and base_symbol in symbols:
                    ticker_data = self._make_request("api/mix/v1/market/ticker", {'symbol': symbol_name})
                    
                    if ticker_data and 'data' in ticker_data:
                        ticker = ticker_data['data']
                        funding_rate = float(ticker.get('fundingRate', 0))
                        
                        funding_data.append(FundingRateData(
                            exchange="Bitget",
                            symbol=base_symbol,
                            rate=funding_rate,
                            timestamp=datetime.now()
                        ))
                    
                    time.sleep(0.1)
                    
        except Exception as e:
            logging.error(f"Bitget funding rates error: {e}")
        
        return funding_data

class KuCoinAPI(ExchangeAPI):
    """API KuCoin"""
    
    def __init__(self):
        super().__init__("https://api-futures.kucoin.com", "KuCoin")
        self.symbol_mapping = {
            'BTC': 'XBTUSDTM', 'ETH': 'ETHUSDTM', 'SOL': 'SOLUSDTM',
            'ADA': 'ADAUSDTM', 'MATIC': 'MATICUSDTM', 'DOT': 'DOTUSDTM', 'AVAX': 'AVAXUSDTM'
        }
    
    def get_funding_rates(self, symbols: List[str]) -> List[FundingRateData]:
        """Récupère les funding rates de KuCoin"""
        funding_data = []
        
        for symbol in symbols:
            try:
                if symbol not in self.symbol_mapping:
                    continue
                
                kucoin_symbol = self.symbol_mapping[symbol]
                data = self._make_request(f"api/v1/funding-rate/{kucoin_symbol}/current")
                
                if data and data.get('code') == '200000' and 'data' in data:
                    item = data['data']
                    funding_rate = float(item.get('value', 0))
                    
                    contract_data = self._make_request(f"api/v1/contracts/{kucoin_symbol}")
                    mark_price = None
                    
                    if contract_data and contract_data.get('code') == '200000':
                        contract_info = contract_data.get('data', {})
                        if 'fundingFeeRate' in contract_info:
                            funding_rate = float(contract_info['fundingFeeRate'])
                        mark_price = float(contract_info.get('markPrice', 0)) if contract_info.get('markPrice') else None
                    
                    funding_data.append(FundingRateData(
                        exchange="KuCoin",
                        symbol=symbol,
                        rate=funding_rate,
                        timestamp=datetime.now(),
                        mark_price=mark_price
                    ))
                
                time.sleep(0.2)
                    
            except Exception as e:
                logging.error(f"KuCoin funding rate error for {symbol}: {e}")
                continue
        
        return funding_data

class FundingRateCollector:
    """Collecteur principal des funding rates"""
    
    def __init__(self):
        self.exchanges = {
            'binance': BinanceAPI(),
            'bybit': BybitAPI(), 
            'okx': OKXAPI(),
            'bitget': BitgetAPI(),
            'kucoin': KuCoinAPI()
        }
        
        # Filtrer seulement les exchanges activés
        enabled = config.ENABLED_EXCHANGES
        self.exchanges = {k: v for k, v in self.exchanges.items() if k in enabled}
        
        self.fee_structure = config.EXCHANGE_FEES
        self.slippage_estimates = config.SLIPPAGE_ESTIMATES
    
    def collect_all_funding_rates(self, symbols: List[str]) -> List[FundingRateData]:
        """Collecte les funding rates de tous les exchanges"""
        all_funding_data = []
        
        logging.info(f"🔍 Collecte des funding rates pour {symbols}")
        
        for exchange_name, api in self.exchanges.items():
            try:
                logging.info(f"📡 Récupération depuis {exchange_name}...")
                rates = api.get_funding_rates(symbols)
                all_funding_data.extend(rates)
                logging.info(f"✅ {exchange_name}: {len(rates)} rates récupérés")
                
            except Exception as e:
                logging.error(f"❌ Erreur {exchange_name}: {e}")
                continue
        
        logging.info(f"📊 Total: {len(all_funding_data)} funding rates collectés")
        return all_funding_data
    
    def find_arbitrage_opportunities(self, funding_data: List[FundingRateData]) -> List[ArbitrageOpportunity]:
        """Trouve les opportunités d'arbitrage"""
        opportunities = []
        
        symbol_groups = {}
        for data in funding_data:
            if data.symbol not in symbol_groups:
                symbol_groups[data.symbol] = []
            symbol_groups[data.symbol].append(data)
        
        for symbol, rates in symbol_groups.items():
            if len(rates) < 2:
                continue
            
            for i in range(len(rates)):
                for j in range(i + 1, len(rates)):
                    rate1, rate2 = rates[i], rates[j]
                    
                    if rate1.rate > rate2.rate:
                        long_rate, short_rate = rate1, rate2
                    else:
                        long_rate, short_rate = rate2, rate1
                    
                    rate_diff = long_rate.rate - short_rate.rate
                    
                    if rate_diff > 0.0001:  # 0.01%
                        opportunity = self._calculate_arbitrage_profit(long_rate, short_rate, symbol)
                        if opportunity.net_profit_8h > 0:
                            opportunities.append(opportunity)
        
        return sorted(opportunities, key=lambda x: x.net_profit_8h, reverse=True)
    
    def _calculate_arbitrage_profit(self, long_rate: FundingRateData, 
                                 short_rate: FundingRateData, symbol: str) -> ArbitrageOpportunity:
        """Calcule le profit d'arbitrage"""
        
        rate_diff = long_rate.rate - short_rate.rate
        
        long_fees = self.fee_structure.get(long_rate.exchange, 0.1) / 100
        short_fees = self.fee_structure.get(short_rate.exchange, 0.1) / 100
        slippage = self.slippage_estimates.get(symbol, 0.05) / 100
        
        total_fees = long_fees + short_fees + (2 * slippage) + 0.002
        gross_profit_8h = rate_diff
        net_profit_8h = gross_profit_8h - total_fees
        
        return ArbitrageOpportunity(
            long_exchange=long_rate.exchange,
            short_exchange=short_rate.exchange,
            symbol=symbol,
            long_rate=long_rate.rate,
            short_rate=short_rate.rate,
            rate_difference=rate_diff,
            potential_profit_8h=gross_profit_8h,
            estimated_fees=total_fees,
            net_profit_8h=net_profit_8h,
            next_funding_time=long_rate.next_funding_time or short_rate.next_funding_time
        )

class FundingRateBot:
    """Bot principal de trading des funding rates"""
    
    def __init__(self):
        self.collector = FundingRateCollector()
        self.symbols = config.SYMBOLS
        self.min_profit_threshold = config.MIN_PROFIT_THRESHOLD
        self.position_size = config.POSITION_SIZE
    
    def scan_opportunities(self):
        """Scanne les opportunités"""
        logging.info("🎯 SCAN DES OPPORTUNITÉS DE FUNDING RATE ARBITRAGE")
        print("=" * 80)
        
        try:
            funding_data = self.collector.collect_all_funding_rates(self.symbols)
            
            if not funding_data:
                logging.warning("❌ Aucune donnée récupérée")
                return
            
            self.display_funding_summary(funding_data)
            opportunities = self.collector.find_arbitrage_opportunities(funding_data)
            
            profitable_ops = [
                op for op in opportunities 
                if op.net_profit_8h >= self.min_profit_threshold
            ]
            
            if profitable_ops:
                print(f"\n💰 {len(profitable_ops)} OPPORTUNITÉS RENTABLES TROUVÉES:")
                self.display_opportunities(profitable_ops[:5])
                self.save_opportunities(profitable_ops)
            else:
                print(f"\n❌ Aucune opportunité rentable trouvée (seuil: {self.min_profit_threshold:.3%})")
                print("📊 Meilleures opportunités actuelles:")
                if opportunities:
                    self.display_opportunities(opportunities[:3])
            
        except Exception as e:
            logging.error(f"Erreur lors du scan: {e}")
    
    def display_funding_summary(self, funding_data: List[FundingRateData]):
        """Affiche un résumé des funding rates collectés"""
        print(f"\n📊 RÉSUMÉ DES FUNDING RATES COLLECTÉS:")
        print("-" * 50)
        
        exchange_data = {}
        for data in funding_data:
            if data.exchange not in exchange_data:
                exchange_data[data.exchange] = []
            exchange_data[data.exchange].append(data)
        
        for exchange, rates in exchange_data.items():
            print(f"{exchange}: {len(rates)} symbols")
            
        print(f"\nTotal: {len(funding_data)} funding rates")
    
    def display_opportunities(self, opportunities: List[ArbitrageOpportunity]):
        """Affiche les opportunités"""
        print("\n" + "="*80)
        print("🎯 TOP OPPORTUNITÉS D'ARBITRAGE")
        print("="*80)
        
        for i, op in enumerate(opportunities, 1):
            profit_usd = op.net_profit_8h * self.position_size
            
            print(f"\n{i}. 🪙 {op.symbol}")
            print(f"   📈 Long:  {op.long_exchange} ({op.long_rate:.4%})")
            print(f"   📉 Short: {op.short_exchange} ({op.short_rate:.4%})")
            print(f"   💵 Écart: {op.rate_difference:.4%}")
            print(f"   💰 Profit Net (8h): {op.net_profit_8h:.3%} = ${profit_usd:.2f}")
            print(f"   💸 Frais estimés: {op.estimated_fees:.3%}")
            
            if op.next_funding_time:
                print(f"   ⏰ Prochain funding: {op.next_funding_time.strftime('%H:%M:%S')}")
            
            print("-" * 50)
    
    def save_opportunities(self, opportunities: List[ArbitrageOpportunity]):
        """Sauvegarde les opportunités"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{config.DATA_DIR}/opportunities_{timestamp}.json"
        
        data = []
        for op in opportunities:
            data.append({
                'timestamp': datetime.now().isoformat(),
                'symbol': op.symbol,
                'long_exchange': op.long_exchange,
                'short_exchange': op.short_exchange,
                'long_rate': op.long_rate,
                'short_rate': op.short_rate,
                'rate_difference': op.rate_difference,
                'net_profit_8h_pct': op.net_profit_8h,
                'estimated_profit_usd': op.net_profit_8h * self.position_size,
                'next_funding_time': op.next_funding_time.isoformat() if op.next_funding_time else None
            })
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        logging.info(f"💾 Opportunités sauvegardées: {filename}")
    
    def run_continuous(self):
        """Lance le bot en mode continu"""
        logging.info("🚀 Démarrage du bot en mode continu")
        
        schedule.every(config.SCAN_INTERVAL).minutes.do(self.scan_opportunities)
        self.scan_opportunities()
        
        print(f"\n⏰ Bot programmé: scan toutes les {config.SCAN_INTERVAL} minutes")
        print("🛑 Ctrl+C pour arrêter")
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)
            except KeyboardInterrupt:
                logging.info("🛑 Arrêt du bot")
                print("\n👋 Bot arrêté")
                break
            except Exception as e:
                logging.error(f"Erreur dans la boucle: {e}")
                time.sleep(300)

def test_connectivity(bot):
    """Test la connectivité aux exchanges"""
    print("\n🔧 TEST DE CONNECTIVITÉ")
    print("-" * 40)
    
    for exchange_name, api in bot.collector.exchanges.items():
        try:
            print(f"📡 Test {exchange_name}...", end=" ")
            rates = api.get_funding_rates(['BTC'])
            if rates:
                print(f"✅ OK ({len(rates)} rates)")
            else:
                print("⚠️  Pas de données")
        except Exception as e:
            print(f"❌ Erreur: {e}")

def main():
    """Fonction principale"""
    
    print("🎯 BOT FUNDING RATE ARBITRAGE - APIS DIRECTES")
    print("=" * 60)
    print("✅ Exchanges supportés: Binance, Bybit, OKX, Bitget, KuCoin")
    print("✅ Aucune clé API requise - 100% GRATUIT")
    print("✅ Données en temps réel directement des exchanges")
    print("=" * 60)
    
    bot = FundingRateBot()
    
    # Gestion des arguments en ligne de commande
    if len(sys.argv) > 1:
        if sys.argv[1] == '--mode' and len(sys.argv) > 2:
            if sys.argv[2] == 'continuous':
                bot.run_continuous()
                return
            elif sys.argv[2] == 'scan':
                bot.scan_opportunities()
                return
        elif sys.argv[1] == '--help':
            print("\nUsage:")
            print("  python funding_rate_bot.py                    # Mode interactif")
            print("  python funding_rate_bot.py --mode continuous  # Mode continu")
            print("  python funding_rate_bot.py --mode scan        # Scan unique")
            print("  python funding_rate_bot.py --help             # Aide")
            return
    
    print("\n🔧 Options disponibles:")
    print("1. 🔍 Scan unique")
    print("2. 🔄 Mode continu (scan toutes les 30 min)")
    print("3. 🧪 Test de connectivité")
    
    choice = input("\nChoix (1/2/3): ").strip()
    
    if choice == "1":
        bot.scan_opportunities()
    elif choice == "2":
        bot.run_continuous()
    elif choice == "3":
        test_connectivity(bot)
    else:
        print("❌ Choix invalide")

if __name__ == "__main__":
    main()
