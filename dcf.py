import sys
import numpy as np
import pandas as pd
import yfinance as yf
import os
import tempfile
import base64
from datetime import datetime
from xlsxwriter.utility import xl_rowcol_to_cell
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QLabel, QLineEdit, QPushButton, 
                             QTableWidget, QTableWidgetItem, QMessageBox, QComboBox, 
                             QGridLayout, QGroupBox, QCheckBox, QTabWidget)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QColor, QBrush, QFont

INST_DEFAULTS = {
    "wacc": "0.09", "tg": "0.03", "tax": "0.21", "nwc": "0.02",
    "target_margin": "0.25", "exit_mult": "15.0",
    "decay": "Exponential", "margin_base": "TTM", "reinv": "D&A = CapEx",
    "discounting": "Mid-Year", "term_method": "Perpetuity Growth"
}

COLOR_GOLD = "#F2C94C" 
COLOR_BLUE = "#569CD6" 
COLOR_WHITE = "#FFFFFF"
COLOR_GREEN = "#4CAF50"
COLOR_RED = "#F44336"
COLOR_GRAY = "#888888"

EXCEL_COLOR_DEFAULT = "#660099" 
EXCEL_COLOR_OVERRIDE = "#0000FF" 

class YFinancePipeline:
    def __init__(self):
        pass

    def extract_all_statements(self, ticker_symbol):
        company = yf.Ticker(ticker_symbol)
        return {
            "income_statement": company.financials,
            "balance_sheet": company.balance_sheet,
            "cash_flow": company.cashflow
        }

    def get_auto_wacc(self, ticker):
        try:
            tnx = yf.Ticker("^TNX").history(period="1d")
            rf_rate = (tnx['Close'].iloc[-1] / 100) if not tnx.empty else 0.042
            beta = yf.Ticker(ticker).info.get("beta", 1.0)
            mrp = 0.055 
            return rf_rate + (beta * mrp)
        except:
            return 0.09 

    def transform_timeline(self, df):
        if df.empty: return df
        df = df[df.columns[::-1]]
        df.columns = [col.year for col in df.columns]
        return df

    def normalize_data(self, df, statement_type):
        if df.empty: return df
        mapping_dictionaries = {
            "income_statement": {
                "Total Revenue": "Revenue", "Operating Revenue": "Revenue",
                "Total Operating Income As Reported": "EBIT (Operating Income)",
                "Tax Provision": "Taxes", "Pretax Income": "Pretax Income",
                "Net Income": "Net Income"
            },
            "balance_sheet": {
                "Total Assets": "Total Assets", "Total Debt": "Total Debt",
                "Cash And Cash Equivalents": "Total Cash", "Ordinary Shares Number": "Shares Outstanding",
                "Stockholders Equity": "Total Equity", "Total Equity Gross Minority Interest": "Total Equity"
            },
            "cash_flow": {
                "Capital Expenditure": "CapEx", "Depreciation And Amortization": "Depreciation & Amortization",
                "Change In Working Capital": "Change in NWC"
            }
        }
        target_map = mapping_dictionaries.get(statement_type, {})
        df_mapped = df.rename(index=target_map)
        rows_to_keep = list(target_map.values())
        df_clean = df_mapped[df_mapped.index.isin(rows_to_keep)]
        return df_clean[~df_clean.index.duplicated(keep='first')].fillna(0)

    def build_projection_engine(self, clean_financials, assumptions):
        is_df = clean_financials["income_statement"]
        cf_df = clean_financials["cash_flow"]

        if is_df.empty or "Revenue" not in is_df.index: return pd.DataFrame()

        hist_years = [col for col in is_df.columns if isinstance(col, int)]
        last_hist_year = hist_years[-1]
        future_years = [last_hist_year + i for i in range(1, 6)]

        revenue = is_df.loc["Revenue"]
        ebit = is_df.loc["EBIT (Operating Income)"]
        taxes = is_df.loc["Taxes"] if "Taxes" in is_df.index else pd.Series(0, index=hist_years)
        da = cf_df.loc["Depreciation & Amortization"] if "Depreciation & Amortization" in cf_df.index else pd.Series(0, index=hist_years)
        capex = cf_df.loc["CapEx"] if "CapEx" in cf_df.index else pd.Series(0, index=hist_years)
        nwc = cf_df.loc["Change in NWC"] if "Change in NWC" in cf_df.index else pd.Series(0, index=hist_years)

        historical_growth_rates = revenue.pct_change()
        
        if assumptions["margin_base"] == "TTM":
            recent_margin = (ebit / revenue).iloc[-1]
        elif assumptions["margin_base"] == "Mean of Available":
            recent_margin = (ebit / revenue).mean()
        else: 
            recent_margin = assumptions["target_margin"]
            
        da_margin = (da / revenue).mean() if revenue.mean() != 0 else 0
        capex_margin = (capex / revenue).mean() if revenue.mean() != 0 else 0
        nwc_margin = assumptions["nwc"]

        dcf_model = pd.DataFrame(index=[
            "Revenue", "Revenue Growth (%)", "EBIT (Operating Income)", "EBIT Margin (%)",
            "Taxes", "NOPAT", "D&A (Add Back)", "CapEx", "Change in NWC", "Unlevered Free Cash Flow"
        ], columns=hist_years + future_years)

        dcf_model.loc["Revenue", hist_years] = revenue
        dcf_model.loc["Revenue Growth (%)", hist_years] = historical_growth_rates
        dcf_model.loc["EBIT (Operating Income)", hist_years] = ebit
        dcf_model.loc["EBIT Margin (%)", hist_years] = ebit / revenue
        dcf_model.loc["Taxes", hist_years] = -abs(taxes)
        dcf_model.loc["NOPAT", hist_years] = ebit - abs(taxes)
        dcf_model.loc["D&A (Add Back)", hist_years] = da
        dcf_model.loc["CapEx", hist_years] = -abs(capex) 
        dcf_model.loc["Change in NWC", hist_years] = nwc
        dcf_model.loc["Unlevered Free Cash Flow", hist_years] = (
            dcf_model.loc["NOPAT", hist_years] + dcf_model.loc["D&A (Add Back)", hist_years] + 
            dcf_model.loc["CapEx", hist_years] + dcf_model.loc["Change in NWC", hist_years]
        )

        last_growth = historical_growth_rates.iloc[-1]
        if pd.isna(last_growth) or last_growth <= 0: last_growth = 0.15 
            
        if assumptions["decay"] == "Exponential" and last_growth > 0:
            decay_curve = np.geomspace(last_growth, assumptions["tg"], num=len(future_years))
        elif assumptions["decay"] == "Flat":
            decay_curve = np.full(len(future_years), last_growth)
        else: 
            decay_curve = np.linspace(last_growth, assumptions["tg"], num=len(future_years))
            
        last_rev = revenue[last_hist_year]
        
        for i, year in enumerate(future_years):
            current_growth = decay_curve[i]
            next_rev = last_rev * (1 + current_growth)
            proj_ebit = next_rev * recent_margin
            proj_taxes = proj_ebit * assumptions["tax"]
            nopat = proj_ebit - proj_taxes
            proj_da = next_rev * abs(da_margin)
            
            if assumptions["reinv"] == "D&A = CapEx": proj_capex = proj_da
            else: proj_capex = next_rev * abs(capex_margin)
                
            proj_nwc = (next_rev - last_rev) * nwc_margin 
            
            dcf_model.loc["Revenue", year] = next_rev
            dcf_model.loc["Revenue Growth (%)", year] = current_growth
            dcf_model.loc["EBIT (Operating Income)", year] = proj_ebit
            dcf_model.loc["EBIT Margin (%)", year] = recent_margin
            dcf_model.loc["Taxes", year] = -proj_taxes
            dcf_model.loc["NOPAT", year] = nopat
            dcf_model.loc["D&A (Add Back)", year] = proj_da
            dcf_model.loc["CapEx", year] = -proj_capex
            dcf_model.loc["Change in NWC", year] = proj_nwc
            dcf_model.loc["Unlevered Free Cash Flow", year] = nopat + proj_da - proj_capex + proj_nwc
            last_rev = next_rev

        return dcf_model

    def build_dupont(self, clean_financials):
        is_df = clean_financials["income_statement"]
        bs_df = clean_financials["balance_sheet"]
        years = [col for col in is_df.columns if isinstance(col, int)]
        
        dupont_df = pd.DataFrame(index=["Net Income", "Revenue", "Total Assets", "Total Equity", 
                                        "Profit Margin (%)", "Asset Turnover (x)", "Equity Multiplier (x)", "ROE (%)"], columns=years)
        
        ni = is_df.loc["Net Income"] if "Net Income" in is_df.index else pd.Series(0, index=years)
        rev = is_df.loc["Revenue"] if "Revenue" in is_df.index else pd.Series(1, index=years)
        assets = bs_df.loc["Total Assets"] if "Total Assets" in bs_df.index else pd.Series(1, index=years)
        debt = bs_df.loc["Total Debt"] if "Total Debt" in bs_df.index else pd.Series(0, index=years)
        equity = bs_df.loc["Total Equity"] if "Total Equity" in bs_df.index else (assets - debt)

        pm = ni / rev.replace(0, np.nan)
        ato = rev / assets.replace(0, np.nan)
        em = assets / equity.replace(0, np.nan)
        roe = pm * ato * em

        dupont_df.loc["Net Income"] = ni
        dupont_df.loc["Revenue"] = rev
        dupont_df.loc["Total Assets"] = assets
        dupont_df.loc["Total Equity"] = equity
        dupont_df.loc["Profit Margin (%)"] = pm.fillna(0)
        dupont_df.loc["Asset Turnover (x)"] = ato.fillna(0)
        dupont_df.loc["Equity Multiplier (x)"] = em.fillna(0)
        dupont_df.loc["ROE (%)"] = roe.fillna(0)
        
        return dupont_df

    def build_comps(self, target_ticker, peers_str):
        tickers = [target_ticker] + [p.strip().upper() for p in peers_str.split(',') if p.strip()]
        metrics = ["Current Price", "Forward P/E", "EV / EBITDA", "Profit Margin (%)", "ROE (%)"]
        comps_df = pd.DataFrame(index=metrics, columns=tickers)
        
        for t in tickers:
            try:
                info = yf.Ticker(t).info
                comps_df.loc["Current Price", t] = info.get("currentPrice", info.get("regularMarketPrice", 0))
                comps_df.loc["Forward P/E", t] = info.get("forwardPE", 0)
                comps_df.loc["EV / EBITDA", t] = info.get("enterpriseToEbitda", 0)
                comps_df.loc["Profit Margin (%)", t] = info.get("profitMargins", 0)
                comps_df.loc["ROE (%)", t] = info.get("returnOnEquity", 0)
            except:
                comps_df[t] = 0
        return comps_df

    def calculate_valuation(self, clean_financials, dcf_model, assumptions):
        bs_df = clean_financials["balance_sheet"]
        latest_year = [col for col in bs_df.columns if isinstance(col, int)][-1]
        
        total_cash = bs_df.loc["Total Cash", latest_year] if "Total Cash" in bs_df.index else 0
        total_debt = bs_df.loc["Total Debt", latest_year] if "Total Debt" in bs_df.index else 0
        shares_out = bs_df.loc["Shares Outstanding", latest_year] if "Shares Outstanding" in bs_df.index else 1
        if shares_out == 0: shares_out = 1 
        
        future_years = [col for col in dcf_model.columns if col > latest_year]
        ufcf_projected = dcf_model.loc["Unlevered Free Cash Flow", future_years].values
        
        wacc = assumptions["wacc"]
        tg = assumptions["tg"]
        
        def get_share_price(w, t):
            if w <= t: return 0 
            
            if assumptions["discounting"] == "Mid-Year": discount_factors = [(1 + w) ** (i - 0.5) for i in range(1, 6)]
            else: discount_factors = [(1 + w) ** i for i in range(1, 6)]
                
            pv_ufcf = sum([cf / df for cf, df in zip(ufcf_projected, discount_factors)])
            
            if assumptions["term_method"] == "Exit Multiple":
                terminal_ebitda = dcf_model.loc["EBIT (Operating Income)", future_years[-1]] + dcf_model.loc["D&A (Add Back)", future_years[-1]]
                terminal_value = terminal_ebitda * assumptions["exit_mult"]
            else: 
                terminal_value = (ufcf_projected[-1] * (1 + t)) / (w - t)
                
            pv_tv = terminal_value / ((1 + w) ** 5)
            ev = pv_ufcf + pv_tv
            return (ev + total_cash - total_debt) / shares_out
            
        wacc_range = [wacc - 0.01, wacc - 0.005, wacc, wacc + 0.005, wacc + 0.01]
        tg_range = [tg - 0.005, tg - 0.0025, tg, tg + 0.0025, tg + 0.005]
        
        sens_df = pd.DataFrame(index=wacc_range, columns=tg_range)
        for w in wacc_range:
            for t in tg_range:
                sens_df.loc[w, t] = get_share_price(w, t)
                
        return {
            "base_share_price": get_share_price(wacc, tg),
            "sensitivity_matrix": sens_df
        }

    def export_live_excel(self, ticker, clean_financials, dcf_model, valuation_data, assumptions, filename, export_routing, current_price, dupont_model=None, comps_model=None, ref_dcf=None, ref_assump=None, ref_sens=None):
        writer = pd.ExcelWriter(filename, engine='xlsxwriter', engine_kwargs={'options': {'nan_inf_to_errors': True}})
        workbook = writer.book
        
        def _s(val):
            if pd.isna(val): return 0.0
            if isinstance(val, (int, float, np.number)) and np.isinf(val): return 0.0
            return val
            
        header = workbook.add_format({'bold': True, 'bg_color': '#D9D9D9', 'bottom': 1, 'font_name': 'Arial'})
        index_format = workbook.add_format({'bold': True, 'font_name': 'Arial', 'font_size': 10})
        currency = workbook.add_format({'num_format': '$#,##0', 'font_name': 'Arial', 'font_size': 10})
        percent = workbook.add_format({'num_format': '0.0%', 'font_name': 'Arial', 'font_size': 10})
        decimal = workbook.add_format({'num_format': '0.00', 'font_name': 'Arial', 'font_size': 10})
        
        def get_fmt(val_key, format_type='pct'):
            is_default = str(assumptions[val_key]) == str(INST_DEFAULTS.get(val_key, ''))
            color = EXCEL_COLOR_DEFAULT if is_default else EXCEL_COLOR_OVERRIDE
            if format_type == 'pct': num_fmt = '0.0%'
            elif format_type == 'curr': num_fmt = '$#,##0'
            elif format_type == 'num': num_fmt = '#,##0.0'
            else: num_fmt = 'General'
            return workbook.add_format({'num_format': num_fmt, 'font_color': color, 'bg_color': '#F2F2F2', 'bold': True, 'font_name': 'Arial'})

        blue_curr = workbook.add_format({'num_format': '$#,##0', 'font_color': EXCEL_COLOR_OVERRIDE, 'font_name': 'Arial', 'font_size': 10})
        blue_pct = workbook.add_format({'num_format': '0.0%', 'font_color': EXCEL_COLOR_OVERRIDE, 'font_name': 'Arial', 'font_size': 10})
        blue_num = workbook.add_format({'num_format': '#,##0', 'font_color': EXCEL_COLOR_OVERRIDE, 'font_name': 'Arial', 'font_size': 10})

        output_green_currency = workbook.add_format({'num_format': '$#,##0.00', 'font_color': '#008000', 'bold': True, 'font_name': 'Arial', 'font_size': 11})
        output_green_ev = workbook.add_format({'num_format': '$#,##0', 'font_color': '#008000', 'bold': True, 'font_name': 'Arial', 'font_size': 11})
        output_green_num = workbook.add_format({'num_format': '#,##0', 'font_color': '#008000', 'bold': True, 'font_name': 'Arial', 'font_size': 11})
        
        market_price_fmt = workbook.add_format({'num_format': '$#,##0.00', 'font_color': '#000000', 'bold': True, 'font_name': 'Arial', 'font_size': 11})
        upside_fmt = workbook.add_format({'num_format': '0.0%', 'font_color': '#008000', 'bold': True, 'font_name': 'Arial'})

        ws_summary = workbook.add_worksheet('Summary')
        ws_summary.set_column('A:A', 25)
        ws_summary.set_column('B:G', 15)
        
        ws_summary.write('A1', f"{ticker} Live Valuation", header)
        ws_summary.write('A3', 'Assumptions (Purple - Institutional Default)', header)
        ws_summary.write('A4', 'WACC')
        ws_summary.write('B4', float(assumptions['wacc']), get_fmt('wacc', 'pct'))
        ws_summary.write('A5', 'Terminal Growth')
        ws_summary.write('B5', float(assumptions['tg']), get_fmt('tg', 'pct'))
        ws_summary.write('A6', 'Tax Rate')
        ws_summary.write('B6', float(assumptions['tax']), get_fmt('tax', 'pct'))
        ws_summary.write('A7', 'Target NWC %')
        ws_summary.write('B7', float(assumptions['nwc']), get_fmt('nwc', 'pct'))
        
        ws_summary.write('D3', 'Architecture Settings', header)
        ws_summary.write('D4', 'Decay Curve')
        ws_summary.write('E4', assumptions['decay'], get_fmt('decay', 'str'))
        ws_summary.write('D5', 'Discounting')
        ws_summary.write('E5', assumptions['discounting'], get_fmt('discounting', 'str'))
        ws_summary.write('D6', 'Terminal Method')
        ws_summary.write('E6', assumptions['term_method'], get_fmt('term_method', 'str'))
        ws_summary.write('D7', 'Exit Multiple')
        ws_summary.write('E7', float(assumptions['exit_mult']), get_fmt('exit_mult', 'num'))
        
        bs_df = clean_financials["balance_sheet"]
        last_col_idx = len(bs_df.columns) 
        def get_bs_ref(metric):
            if metric in bs_df.index: return f"='Balance Sheet'!{xl_rowcol_to_cell(bs_df.index.get_loc(metric) + 1, last_col_idx)}"
            return "=0"

        ws_summary.write('A9', 'Capital Structure (Green = Linked)', header)
        ws_summary.write('A10', 'Total Cash')
        ws_summary.write_formula('B10', get_bs_ref('Total Cash'), output_green_ev)
        ws_summary.write('A11', 'Total Debt')
        ws_summary.write_formula('B11', get_bs_ref('Total Debt'), output_green_ev)
        ws_summary.write('A12', 'Shares Out')
        ws_summary.write_formula('B12', get_bs_ref('Shares Outstanding'), output_green_num)
        
        # DYNAMIC SPATIAL REFERENCING FIX
        hist_cols = len([c for c in clean_financials["income_statement"].columns if isinstance(c, int)])
        idx_start = hist_cols + 1
        idx_end = hist_cols + 5
        
        ufcf_start = xl_rowcol_to_cell(10, idx_start)
        ufcf_end = xl_rowcol_to_cell(10, idx_end)
        ebit_end = xl_rowcol_to_cell(3, idx_end)
        da_end = xl_rowcol_to_cell(7, idx_end)

        ws_summary.write('A14', 'Implied Enterprise Value', header)
        if assumptions["term_method"] == "Perpetuity Growth":
            if assumptions["discounting"] == "Mid-Year": 
                formula_ev = f'=NPV(B4, \'DCF Engine\'!{ufcf_start}:{ufcf_end})*(1+B4)^0.5 + (\'DCF Engine\'!{ufcf_end}*(1+B5))/(B4-B5)/(1+B4)^5'
            else: 
                formula_ev = f'=NPV(B4, \'DCF Engine\'!{ufcf_start}:{ufcf_end}) + (\'DCF Engine\'!{ufcf_end}*(1+B5))/(B4-B5)/(1+B4)^5'
        else: 
            if assumptions["discounting"] == "Mid-Year": 
                formula_ev = f'=NPV(B4, \'DCF Engine\'!{ufcf_start}:{ufcf_end})*(1+B4)^0.5 + (\'DCF Engine\'!{ebit_end}+\'DCF Engine\'!{da_end})*E7/(1+B4)^5'
            else: 
                formula_ev = f'=NPV(B4, \'DCF Engine\'!{ufcf_start}:{ufcf_end}) + (\'DCF Engine\'!{ebit_end}+\'DCF Engine\'!{da_end})*E7/(1+B4)^5'
                
        ws_summary.write_formula('B14', formula_ev, output_green_ev)
        
        ws_summary.write('A15', 'Implied Share Price', header)
        ws_summary.write_formula('B15', '=IF(B12=0, 0, (B14+B10-B11)/B12)', output_green_currency)

        ws_summary.write('A16', 'Current Market Price', header)
        ws_summary.write('B16', current_price, market_price_fmt)
        ws_summary.write('A17', 'Upside / (Downside)', header)
        ws_summary.write_formula('B17', '=IF(B16=0, 0, (B15-B16)/B16)', upside_fmt)

        sens_df = valuation_data["sensitivity_matrix"]
        if export_routing.get('sens'):
            ws_summary.write('A19', 'Sensitivity Matrix: WACC vs Terminal Growth', header)
            for col_num, tg_val in enumerate(sens_df.columns):
                ws_summary.write(19, col_num + 1, tg_val, percent)
            for row_num, wacc_val in enumerate(sens_df.index):
                ws_summary.write(20 + row_num, 0, wacc_val, percent)
                for col_num, tg_val in enumerate(sens_df.columns):
                    price = _s(sens_df.loc[wacc_val, tg_val])
                    ws_summary.write(20 + row_num, col_num + 1, price, workbook.add_format({'num_format': '$#,##0.00', 'font_name': 'Arial'}))

        dcf_model.to_excel(writer, sheet_name='DCF Engine')
        ws_dcf = writer.sheets['DCF Engine']
        ws_dcf.set_column('A:A', 30, index_format)
        ws_dcf.set_column('B:Z', 15)
        
        for r in range(dcf_model.shape[0]):
            row_name = dcf_model.index[r]
            is_pct = "%)" in row_name
            for c in range(dcf_model.shape[1]):
                val = _s(dcf_model.iloc[r, c])
                is_hist = c < hist_cols
                if is_pct: fmt = blue_pct if is_hist else percent
                else: fmt = blue_curr if is_hist else currency
                ws_dcf.write(r+1, c+1, val, fmt)

        for sheet_name, df_data in [('Income Statement', clean_financials["income_statement"]),
                                    ('Balance Sheet', clean_financials["balance_sheet"]),
                                    ('Cash Flow', clean_financials["cash_flow"])]:
            df_data.to_excel(writer, sheet_name=sheet_name)
            ws_raw = writer.sheets[sheet_name]
            ws_raw.set_column('A:A', 30, index_format)
            ws_raw.set_column('B:Z', 15)
            for r in range(df_data.shape[0]):
                for c in range(df_data.shape[1]):
                    val = _s(df_data.iloc[r, c])
                    fmt = blue_num if "Shares" in str(df_data.index[r]) else blue_curr
                    ws_raw.write(r+1, c+1, val, fmt)

        if export_routing.get('dupont') and dupont_model is not None:
            dupont_model.to_excel(writer, sheet_name='DuPont Analysis')
            ws_dup = writer.sheets['DuPont Analysis']
            ws_dup.set_column('A:A', 30, index_format)
            ws_dup.set_column('B:Z', 15)
            for r in range(dupont_model.shape[0]):
                row_name = dupont_model.index[r]
                is_pct = "%)" in row_name
                is_mult = "(x)" in row_name
                for c in range(dupont_model.shape[1]):
                    val = _s(dupont_model.iloc[r, c])
                    if is_pct: fmt = percent
                    elif is_mult: fmt = decimal
                    else: fmt = currency
                    ws_dup.write(r+1, c+1, val, fmt)

        if export_routing.get('comps') and comps_model is not None:
            comps_model.to_excel(writer, sheet_name='Comps Analysis')
            ws_comp = writer.sheets['Comps Analysis']
            ws_comp.set_column('A:A', 25, index_format)
            ws_comp.set_column('B:Z', 15)
            for r in range(comps_model.shape[0]):
                row_name = comps_model.index[r]
                is_pct = "%)" in row_name
                is_price = "Price" in row_name
                for c in range(comps_model.shape[1]):
                    val = _s(comps_model.iloc[r, c])
                    if is_pct: fmt = percent
                    elif is_price: fmt = workbook.add_format({'num_format': '$#,##0.00', 'font_name': 'Arial', 'font_size': 10})
                    else: fmt = decimal
                    ws_comp.write(r+1, c+1, val, fmt)

        if ref_dcf is not None and ref_assump is not None:
            ws_comp = workbook.add_worksheet('Scenario Comparison')
            ws_comp.set_column('A:A', 35, index_format)
            ws_comp.set_column('B:Z', 15)
            
            c_red = workbook.add_format({'font_color': '#FF0000', 'num_format': '$#,##0', 'font_name': 'Arial', 'bold': True})
            c_green = workbook.add_format({'font_color': '#008000', 'num_format': '$#,##0', 'font_name': 'Arial', 'bold': True})
            c_black = workbook.add_format({'font_color': '#000000', 'num_format': '$#,##0', 'font_name': 'Arial'})
            
            c_red_pct = workbook.add_format({'font_color': '#FF0000', 'num_format': '0.0%', 'font_name': 'Arial', 'bold': True})
            c_green_pct = workbook.add_format({'font_color': '#008000', 'num_format': '0.0%', 'font_name': 'Arial', 'bold': True})
            c_black_pct = workbook.add_format({'font_color': '#000000', 'num_format': '0.0%', 'font_name': 'Arial'})
            
            c_red_sens = workbook.add_format({'font_color': '#FF0000', 'num_format': '$#,##0.00', 'font_name': 'Arial', 'bold': True})
            c_green_sens = workbook.add_format({'font_color': '#008000', 'num_format': '$#,##0.00', 'font_name': 'Arial', 'bold': True})
            c_black_sens = workbook.add_format({'font_color': '#000000', 'num_format': '$#,##0.00', 'font_name': 'Arial'})

            ws_comp.write('A1', 'Scenario Comparison Log', workbook.add_format({'bold': True, 'font_size': 14, 'font_name': 'Arial'}))
            
            ws_comp.write('A3', 'Assumptions Log', header)
            ws_comp.write('B3', 'Previous (Reference)', header)
            ws_comp.write('C3', 'Current (Retained)', header)
            
            row_idx = 4
            for key in assumptions.keys():
                ws_comp.write(row_idx, 0, key)
                ws_comp.write(row_idx, 1, str(ref_assump.get(key, 'N/A')))
                ws_comp.write(row_idx, 2, str(assumptions[key]))
                row_idx += 1
                
            row_idx += 2
            ws_comp.write(row_idx, 0, 'DCF Delta Matrix', header)
            for c_idx, col_name in enumerate(dcf_model.columns): 
                ws_comp.write(row_idx, c_idx + 1, col_name, header)
                
            for r in range(dcf_model.shape[0]):
                row_name = dcf_model.index[r]
                is_pct_row = "%)" in row_name
                
                ws_comp.write(row_idx + 1, 0, f"{row_name} ($/Abs Diff)", index_format)
                ws_comp.write(row_idx + 2, 0, f"{row_name} (% Diff)", index_format)
                
                for c in range(dcf_model.shape[1]):
                    try:
                        new_val = float(dcf_model.iloc[r, c])
                        old_val = float(ref_dcf.iloc[r, c])
                    except: new_val, old_val = 0.0, 0.0
                    new_val = _s(new_val)
                    old_val = _s(old_val)
                    
                    diff_abs = new_val - old_val
                    diff_pct = (diff_abs / abs(old_val)) if old_val != 0 else 0.0
                    diff_pct = _s(diff_pct)
                    
                    if diff_abs > 0: fmt_abs = c_green_pct if is_pct_row else c_green
                    elif diff_abs < 0: fmt_abs = c_red_pct if is_pct_row else c_red
                    else: fmt_abs = c_black_pct if is_pct_row else c_black
                        
                    if diff_pct > 0: fmt_pct = c_green_pct
                    elif diff_pct < 0: fmt_pct = c_red_pct
                    else: fmt_pct = c_black_pct
                        
                    ws_comp.write(row_idx + 1, c + 1, diff_abs, fmt_abs)
                    ws_comp.write(row_idx + 2, c + 1, diff_pct, fmt_pct)
                row_idx += 2

            if export_routing.get('sens') and ref_sens is not None:
                row_idx += 3
                ws_comp.write(row_idx, 0, 'Sensitivity Delta ($ Diff)', header)
                for c_idx, col_name in enumerate(sens_df.columns):
                    ws_comp.write(row_idx, c_idx + 1, col_name, percent)
                for r in range(sens_df.shape[0]):
                    row_name = sens_df.index[r]
                    ws_comp.write(row_idx + r + 1, 0, row_name, percent)
                    for c in range(sens_df.shape[1]):
                        try: new_val, old_val = float(sens_df.iloc[r, c]), float(ref_sens.iloc[r, c])
                        except: new_val, old_val = 0.0, 0.0
                        new_val = _s(new_val)
                        old_val = _s(old_val)
                        
                        diff = new_val - old_val
                        if diff > 0: fmt = c_green_sens
                        elif diff < 0: fmt = c_red_sens
                        else: fmt = c_black_sens
                        ws_comp.write(row_idx + r + 1, c + 1, diff, fmt)
                        
                row_idx += sens_df.shape[0] + 3
                ws_comp.write(row_idx, 0, 'Sensitivity Delta (% Diff)', header)
                for c_idx, col_name in enumerate(sens_df.columns):
                    ws_comp.write(row_idx, c_idx + 1, col_name, percent)
                for r in range(sens_df.shape[0]):
                    row_name = sens_df.index[r]
                    ws_comp.write(row_idx + r + 1, 0, row_name, percent)
                    for c in range(sens_df.shape[1]):
                        try: new_val, old_val = float(sens_df.iloc[r, c]), float(ref_sens.iloc[r, c])
                        except: new_val, old_val = 0.0, 0.0
                        new_val = _s(new_val)
                        old_val = _s(old_val)
                        
                        diff_pct = (new_val - old_val) / abs(old_val) if old_val != 0 else 0.0
                        diff_pct = _s(diff_pct)
                        
                        if diff_pct > 0: fmt = c_green_pct
                        elif diff_pct < 0: fmt = c_red_pct
                        else: fmt = c_black_pct
                        ws_comp.write(row_idx + r + 1, c + 1, diff_pct, fmt)

        writer.close()
        os.startfile(filename) 

class DCFDashboard(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Institutional Equity Research Terminal")
        self.setGeometry(100, 100, 1400, 900)
        
        self.temp_dir = tempfile.gettempdir()
        self.dark_check_path = os.path.join(self.temp_dir, "dark_check.png").replace('\\', '/')
        self.light_check_path = os.path.join(self.temp_dir, "light_check.png").replace('\\', '/')
        
        dark_b64 = b"iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAT0lEQVR4nOWQMQoAMAgDtf//s91EjKnYqdBsweRERb6URbMuyw6ZAgA2AVjyygA5SMsVAG4kQFcE5CArKjXdtiIPJ0Cgm1VPPEFGqh76oDYPaA8KAPLy8AAAAABJRU5ErkJggg=="
        light_b64 = b"iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAVUlEQVR4nOWQOw4AIAhDi/e/M64IBdTJxG6E9vEB/pOqqq3HTdhCjgAMtg3wq4uIUIA3VuEAYDcyoNXIjFnQTgeApeim+fCyQWboeuGJFeRI7KFvagIiIjvyKSEChAAAAABJRU5ErkJggg=="
        
        with open(self.dark_check_path, "wb") as f: f.write(base64.b64decode(dark_b64))
        with open(self.light_check_path, "wb") as f: f.write(base64.b64decode(light_b64))

        self.pipeline = YFinancePipeline()
        self.clean_financials = None
        self.current_ticker = None
        self.current_info = {}
        
        self.ref_dcf = None
        self.ref_sens = None
        self.ref_assump = None
        
        self.is_dark_mode = True
        self.tab_fonts = {'dcf': 10, 'sens': 10, 'dupont': 10, 'comps': 10}
        
        self.user_core = {
            "WACC": INST_DEFAULTS["wacc"], "Term Growth": INST_DEFAULTS["tg"], 
            "Tax Rate": INST_DEFAULTS["tax"], "Target NWC %": INST_DEFAULTS["nwc"],
            "Target Margin %": INST_DEFAULTS["target_margin"], "Exit Multiple": INST_DEFAULTS["exit_mult"]
        }
        self.user_arch = {
            "Decay Curve": INST_DEFAULTS["decay"], "Margin Base": INST_DEFAULTS["margin_base"], 
            "Reinvestment": INST_DEFAULTS["reinv"], "Discounting": INST_DEFAULTS["discounting"], 
            "Terminal Method": INST_DEFAULTS["term_method"]
        }
        
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout(main_widget)
        
        top_bar = QHBoxLayout()
        group_nums = QGroupBox("Core Assumptions")
        layout_nums = QGridLayout()
        self.inputs = {}
        
        layout_nums.addWidget(QLabel("Ticker:"), 0, 0)
        self.input_ticker = QLineEdit("PLTR")
        self.input_ticker.setFixedWidth(80)
        layout_nums.addWidget(self.input_ticker, 1, 0)
        
        layout_nums.addWidget(QLabel("WACC:"), 0, 1)
        self.input_wacc = QLineEdit(self.user_core["WACC"])
        self.input_wacc.setFixedWidth(80)
        self.input_wacc.textChanged.connect(self.update_colors)
        self.inputs["WACC"] = self.input_wacc
        layout_nums.addWidget(self.input_wacc, 1, 1)
        
        self.chk_auto_capm = QCheckBox("Auto-CAPM")
        self.chk_auto_capm.toggled.connect(self.toggle_auto_capm)
        layout_nums.addWidget(self.chk_auto_capm, 2, 1)
        
        row1_labels = [("Term Growth", self.user_core["Term Growth"]), ("Tax Rate", self.user_core["Tax Rate"]), 
                       ("Target NWC %", self.user_core["Target NWC %"]), ("Target Margin %", self.user_core["Target Margin %"]), 
                       ("Exit Multiple", self.user_core["Exit Multiple"])]
        
        for i, (name, default) in enumerate(row1_labels, start=2):
            layout_nums.addWidget(QLabel(name + ":"), 0, i)
            line_edit = QLineEdit(default)
            line_edit.setFixedWidth(80)
            line_edit.textChanged.connect(self.update_colors)
            self.inputs[name] = line_edit
            layout_nums.addWidget(line_edit, 1, i)
            
        self.chk_auto_tax = QCheckBox("Auto-Tax")
        self.chk_auto_tax.toggled.connect(self.toggle_auto_tax)
        layout_nums.addWidget(self.chk_auto_tax, 2, 3)
            
        btn_set_core = QPushButton("Set Defaults")
        btn_set_core.clicked.connect(self.set_core_defaults)
        btn_reset_core = QPushButton("Reset Core")
        btn_reset_core.clicked.connect(self.reset_core_defaults)
        layout_nums.addWidget(btn_set_core, 2, 0)
        layout_nums.addWidget(btn_reset_core, 2, 5)
        
        group_nums.setLayout(layout_nums)
        top_bar.addWidget(group_nums, stretch=2)
        
        group_arch = QGroupBox("Architecture Settings")
        layout_arch = QGridLayout()
        self.dropdowns = {}
        row2_configs = [
            ("Decay Curve", ["Exponential", "Linear", "Flat"], self.user_arch["Decay Curve"]),
            ("Margin Base", ["TTM", "Mean of Available", "Manual Target"], self.user_arch["Margin Base"]),
            ("Reinvestment", ["D&A = CapEx", "Historical Average"], self.user_arch["Reinvestment"]),
            ("Discounting", ["Mid-Year", "Year-End"], self.user_arch["Discounting"]),
            ("Terminal Method", ["Perpetuity Growth", "Exit Multiple"], self.user_arch["Terminal Method"])
        ]
        for i, (name, options, default) in enumerate(row2_configs):
            layout_arch.addWidget(QLabel(name + ":"), 0, i)
            combo = QComboBox()
            combo.addItems(options)
            combo.setCurrentText(default)
            combo.currentTextChanged.connect(self.update_colors)
            self.dropdowns[name] = combo
            layout_arch.addWidget(combo, 1, i)
            
        btn_set_arch = QPushButton("Set Arch Defaults")
        btn_set_arch.clicked.connect(self.set_arch_defaults)
        btn_reset_arch = QPushButton("Reset Arch")
        btn_reset_arch.clicked.connect(self.reset_arch_defaults)
        
        layout_arch.addWidget(btn_set_arch, 2, 0, 1, 2)
        layout_arch.addWidget(btn_reset_arch, 2, 2, 1, 2)
            
        group_arch.setLayout(layout_arch)
        top_bar.addWidget(group_arch, stretch=2)
        
        group_routing = QGroupBox("Advanced Modules Routing")
        layout_routing = QGridLayout()
        
        layout_routing.addWidget(QLabel("Module"), 0, 0)
        layout_routing.addWidget(QLabel("GUI"), 0, 1)
        layout_routing.addWidget(QLabel("Excel"), 0, 2)
        
        self.routing_chks = {}
        modules = [("sens", "Sensitivity Matrix"), ("dupont", "DuPont / ROIC"), ("comps", "Comps Analysis")]
        
        for r, (key, label) in enumerate(modules, start=1):
            layout_routing.addWidget(QLabel(label), r, 0)
            chk_gui = QCheckBox()
            chk_gui.setChecked(True)
            chk_gui.toggled.connect(self.update_tabs_visibility)
            chk_excel = QCheckBox()
            chk_excel.setChecked(True)
            self.routing_chks[key] = {'gui': chk_gui, 'excel': chk_excel}
            layout_routing.addWidget(chk_gui, r, 1)
            layout_routing.addWidget(chk_excel, r, 2)
            
        layout_routing.addWidget(QLabel("Peers:"), 4, 0)
        self.input_peers = QLineEdit("SNOW, DDOG, CRWD")
        layout_routing.addWidget(self.input_peers, 4, 1, 1, 2)
            
        group_routing.setLayout(layout_routing)
        top_bar.addWidget(group_routing, stretch=1)
        
        layout.addLayout(top_bar)
        
        headline_layout = QHBoxLayout()
        
        self.btn_run = QPushButton("1. Extract & Calculate")
        self.btn_run.setObjectName("runBtn")
        self.btn_run.setFixedWidth(200)
        self.btn_run.clicked.connect(self.run_pipeline)
        headline_layout.addWidget(self.btn_run)
        
        self.btn_export = QPushButton("2. Export Live Excel")
        self.btn_export.setObjectName("exportBtn")
        self.btn_export.setFixedWidth(200)
        self.btn_export.clicked.connect(self.export_excel)
        self.btn_export.setEnabled(False)
        headline_layout.addWidget(self.btn_export)
        
        self.chk_compare = QCheckBox("Scenario Comparison Mode")
        self.chk_compare.toggled.connect(self.toggle_comparison)
        headline_layout.addWidget(self.chk_compare)
        
        headline_layout.addStretch(1)
        
        self.btn_theme = QPushButton("☀️ Light Mode")
        self.btn_theme.setFixedWidth(120)
        self.btn_theme.clicked.connect(self.toggle_theme)
        headline_layout.addWidget(self.btn_theme)
        
        self.lbl_share_price = QLabel("Implied Price: $--  |  Market Price: $--")
        self.lbl_share_price.setAlignment(Qt.AlignmentFlag.AlignRight)
        self.lbl_share_price.setStyleSheet("font-size: 18px; font-weight: bold;")
        headline_layout.addWidget(self.lbl_share_price)
        
        layout.addLayout(headline_layout)
        
        self.tabs = QTabWidget()
        self.tab_dcf = QWidget()
        vbox_dcf = QVBoxLayout(self.tab_dcf)
        self.table_main = QTableWidget()
        vbox_dcf.addLayout(self.make_zoom_controls('dcf'))
        vbox_dcf.addWidget(self.table_main)
        self.tabs.addTab(self.tab_dcf, "DCF Engine")
        
        self.tab_sens = QWidget()
        vbox_sens = QVBoxLayout(self.tab_sens)
        self.table_sens = QTableWidget()
        vbox_sens.addLayout(self.make_zoom_controls('sens'))
        vbox_sens.addWidget(self.table_sens)
        self.tabs.addTab(self.tab_sens, "Sensitivity Matrix")
        
        self.tab_dupont = QWidget()
        vbox_dupont = QVBoxLayout(self.tab_dupont)
        self.table_dupont = QTableWidget()
        vbox_dupont.addLayout(self.make_zoom_controls('dupont'))
        vbox_dupont.addWidget(self.table_dupont)
        self.tabs.addTab(self.tab_dupont, "DuPont Analysis")
        
        self.tab_comps = QWidget()
        vbox_comps = QVBoxLayout(self.tab_comps)
        self.table_comps = QTableWidget()
        vbox_comps.addLayout(self.make_zoom_controls('comps'))
        vbox_comps.addWidget(self.table_comps)
        self.tabs.addTab(self.tab_comps, "Comps Analysis")
        
        layout.addWidget(self.tabs)
        
        self.setStyleSheet(self.get_stylesheet())
        self.update_colors()

    @property
    def color_default(self): return COLOR_GOLD if self.is_dark_mode else EXCEL_COLOR_DEFAULT
    @property
    def color_override(self): return COLOR_BLUE if self.is_dark_mode else EXCEL_COLOR_OVERRIDE
    @property
    def color_text(self): return COLOR_WHITE if self.is_dark_mode else "#000000"

    def get_stylesheet(self):
        if self.is_dark_mode:
            bg_main = "#121212"
            bg_widget = "#1E1E1E"
            fg_main = "#FFFFFF"
            grid_color = "#333333"
            chk_bg = "white"
            chk_img = f"url({self.dark_check_path})"
        else:
            bg_main = "#F0F0F0"
            bg_widget = "#FFFFFF"
            fg_main = "#000000"
            grid_color = "#CCCCCC"
            chk_bg = "black"
            chk_img = f"url({self.light_check_path})"

        return f"""
        QMainWindow, QWidget {{ background-color: {bg_main}; color: {fg_main}; font-family: Arial; }}
        QGroupBox {{ border: 1px solid {grid_color}; margin-top: 1ex; font-weight: bold; }}
        QGroupBox::title {{ subcontrol-origin: margin; subcontrol-position: top left; padding: 0 3px; color: #888888; }}
        QPushButton {{ background-color: #333333; color: white; border: 1px solid #555555; padding: 4px; border-radius: 3px; font-weight: bold;}}
        QPushButton:hover {{ background-color: #444444; }}
        QPushButton#runBtn {{ background-color: #007ACC; font-weight: bold; padding: 6px; font-size: 14px;}}
        QPushButton#runBtn:hover {{ background-color: #0098FF; }}
        QPushButton#exportBtn {{ background-color: #107C41; font-weight: bold; padding: 6px; font-size: 14px;}}
        QPushButton#exportBtn:hover {{ background-color: #128E4A; }}
        QTableWidget {{ background-color: {bg_widget}; color: {fg_main}; gridline-color: {grid_color}; border: 1px solid {grid_color}; }}
        QHeaderView::section {{ background-color: {bg_widget}; color: {fg_main}; padding: 4px; border: 1px solid {grid_color}; font-weight: bold; }}
        QTableCornerButton::section {{ background-color: {bg_widget}; border: 1px solid {grid_color}; }}
        QComboBox QAbstractItemView {{ background-color: {bg_widget}; color: {fg_main}; selection-background-color: #007ACC; }}
        QTabWidget::pane {{ border: 1px solid {grid_color}; }}
        QTabBar::tab:selected {{ background-color: #2D89EF; color: white; }}
        QTabBar::tab:!selected {{ background-color: {bg_widget}; color: #888888; }}
        QCheckBox {{ color: {fg_main}; font-weight: bold; padding-left: 2px; }}
        QCheckBox::indicator {{ width: 14px; height: 14px; background-color: {chk_bg}; border: 1px solid #888; border-radius: 2px; }}
        QCheckBox::indicator:checked {{ image: {chk_img}; }}
        """

    def toggle_theme(self):
        self.is_dark_mode = not self.is_dark_mode
        self.btn_theme.setText("☀️ Light Mode" if self.is_dark_mode else "🌙 Dark Mode")
        self.setStyleSheet(self.get_stylesheet())
        self.update_colors()
        for tab_key in ['dcf', 'sens', 'dupont', 'comps']: self.resize_tab_table(tab_key, 0)
        
        if self.clean_financials is not None:
            self.populate_main_table(self.dcf_model)
            self.populate_sens_table(self.valuation_data["sensitivity_matrix"])
            if hasattr(self, 'dupont_model'): self.populate_dupont_table(self.dupont_model)
            if hasattr(self, 'comps_model'): self.populate_comps_table(self.comps_model)

    def make_zoom_controls(self, tab_key):
        hbox = QHBoxLayout()
        hbox.addStretch()
        btn_plus = QPushButton("+")
        btn_minus = QPushButton("-")
        btn_plus.setFixedSize(25, 25)
        btn_minus.setFixedSize(25, 25)
        btn_plus.clicked.connect(lambda: self.resize_tab_table(tab_key, 1))
        btn_minus.clicked.connect(lambda: self.resize_tab_table(tab_key, -1))
        hbox.addWidget(btn_plus)
        hbox.addWidget(btn_minus)
        return hbox

    def resize_tab_table(self, tab_key, delta):
        table_map = {'dcf': self.table_main, 'sens': self.table_sens, 'dupont': self.table_dupont, 'comps': self.table_comps}
        table = table_map[tab_key]
        self.tab_fonts[tab_key] = max(6, min(24, self.tab_fonts[tab_key] + delta))
        fs = self.tab_fonts[tab_key]
        
        font = QFont(table.font())
        font.setPointSize(fs)
        table.setFont(font)
        
        bg_hdr = "#2D2D30" if self.is_dark_mode else "#E0E0E0"
        fg_hdr = "#FFFFFF" if self.is_dark_mode else "#000000"
        border_hdr = "#333333" if self.is_dark_mode else "#CCCCCC"
        hdr_style = f"QHeaderView::section {{ background-color: {bg_hdr}; color: {fg_hdr}; padding: 4px; border: 1px solid {border_hdr}; font-weight: bold; font-size: {fs}pt; }}"
        
        table.horizontalHeader().setStyleSheet(hdr_style)
        table.verticalHeader().setStyleSheet(hdr_style)
        table.resizeColumnsToContents()
        table.resizeRowsToContents()

    def update_tabs_visibility(self):
        self.tabs.setTabVisible(1, self.routing_chks['sens']['gui'].isChecked())
        self.tabs.setTabVisible(2, self.routing_chks['dupont']['gui'].isChecked())
        self.tabs.setTabVisible(3, self.routing_chks['comps']['gui'].isChecked())

    def toggle_auto_capm(self, checked):
        self.input_wacc.setReadOnly(checked)
        self.update_colors()

    def toggle_auto_tax(self, checked):
        self.inputs["Tax Rate"].setReadOnly(checked)
        self.update_colors()

    def toggle_comparison(self, checked):
        if checked:
            if self.clean_financials is None:
                QMessageBox.warning(self, "Warning", "Calculate a baseline before enabling Comparison Mode.")
                self.chk_compare.setChecked(False)
                return
            self.ref_dcf = self.dcf_model.copy()
            self.ref_sens = self.valuation_data["sensitivity_matrix"].copy()
            self.ref_assump = self.get_assumptions()
            QMessageBox.information(self, "Comparison Mode", "Values locked. Change assumptions and recalculate to view delta.")
        else:
            self.ref_dcf = None; self.ref_sens = None; self.ref_assump = None
            if self.clean_financials is not None:
                self.populate_main_table(self.dcf_model)
                self.populate_sens_table(self.valuation_data["sensitivity_matrix"])

    def update_colors(self):
        bg_color = "#1E1E1E" if self.is_dark_mode else "#FFFFFF"
        border_color = "#333333" if self.is_dark_mode else "#CCCCCC"
        base_css = f"background-color: {bg_color}; font-weight: bold; border: 1px solid {border_color}; padding: 4px; color: "
        
        self.input_ticker.setStyleSheet(base_css + self.color_text + ";")
        self.input_peers.setStyleSheet(base_css + self.color_text + ";")
        
        mapping = {"WACC": "wacc", "Term Growth": "tg", "Tax Rate": "tax", "Target NWC %": "nwc", "Target Margin %": "target_margin", "Exit Multiple": "exit_mult"}
        for name, widget in self.inputs.items():
            if name == "WACC" and self.chk_auto_capm.isChecked(): widget.setStyleSheet(base_css + f"{COLOR_GRAY};"); continue
            if name == "Tax Rate" and self.chk_auto_tax.isChecked(): widget.setStyleSheet(base_css + f"{COLOR_GRAY};"); continue
            color = self.color_default if widget.text() == INST_DEFAULTS[mapping[name]] else self.color_override
            widget.setStyleSheet(base_css + f"{color};")
            
        dropdown_map = {"Decay Curve": "decay", "Margin Base": "margin_base", "Reinvestment": "reinv", "Discounting": "discounting", "Terminal Method": "term_method"}
        for name, combo in self.dropdowns.items():
            check_val = "TTM" if combo.currentText() == "Mean of Available" and INST_DEFAULTS[dropdown_map[name]] == "TTM" else combo.currentText()
            color = self.color_default if check_val == INST_DEFAULTS[dropdown_map[name]] else self.color_override
            combo.setStyleSheet(base_css + f"{color};")

    def set_core_defaults(self):
        for name, widget in self.inputs.items(): self.user_core[name] = widget.text()
    def reset_core_defaults(self):
        for name, widget in self.inputs.items(): widget.setText(self.user_core[name])
    def set_arch_defaults(self):
        for name, widget in self.dropdowns.items(): self.user_arch[name] = widget.currentText()
    def reset_arch_defaults(self):
        for name, widget in self.dropdowns.items(): widget.setCurrentText(self.user_arch[name])

    def get_assumptions(self):
        return {
            "wacc": float(self.inputs["WACC"].text()), "tg": float(self.inputs["Term Growth"].text()), "tax": float(self.inputs["Tax Rate"].text()),
            "nwc": float(self.inputs["Target NWC %"].text()), "target_margin": float(self.inputs["Target Margin %"].text()), "exit_mult": float(self.inputs["Exit Multiple"].text()),
            "decay": self.dropdowns["Decay Curve"].currentText(), "margin_base": self.dropdowns["Margin Base"].currentText(),
            "reinv": self.dropdowns["Reinvestment"].currentText(), "discounting": self.dropdowns["Discounting"].currentText(),
            "term_method": self.dropdowns["Terminal Method"].currentText()
        }

    def run_pipeline(self):
        ticker = self.input_ticker.text().upper()
        try:
            if self.clean_financials is None or self.current_ticker != ticker:
                self.current_ticker = ticker
                self.chk_compare.setChecked(False) 
                
                raw = self.pipeline.extract_all_statements(ticker)
                self.clean_financials = {name: self.pipeline.normalize_data(self.pipeline.transform_timeline(df), name) for name, df in raw.items()}
                self.current_info = yf.Ticker(ticker).info
                
            if self.chk_auto_tax.isChecked():
                is_df = self.clean_financials["income_statement"]
                if "Taxes" in is_df.index and "Pretax Income" in is_df.index:
                    hist_tax_rate = (is_df.loc["Taxes"] / is_df.loc["Pretax Income"]).median()
                    if 0 < hist_tax_rate < 0.35: self.inputs["Tax Rate"].setText(f"{hist_tax_rate:.3f}")
            
            if self.chk_auto_capm.isChecked():
                auto_wacc = self.pipeline.get_auto_wacc(ticker)
                self.inputs["WACC"].setText(f"{auto_wacc:.4f}")
            
            assumptions = self.get_assumptions()
            self.dcf_model = self.pipeline.build_projection_engine(self.clean_financials, assumptions)
            self.valuation_data = self.pipeline.calculate_valuation(self.clean_financials, self.dcf_model, assumptions)
            
            implied = self.valuation_data['base_share_price']
            current_price = self.current_info.get("currentPrice", self.current_info.get("regularMarketPrice", 0))
            
            if current_price > 0:
                delta_pct = (implied - current_price) / current_price
                color = COLOR_GREEN if delta_pct > 0 else COLOR_RED
                delta_str = f"(+{delta_pct:.1%} Upside)" if delta_pct > 0 else f"({delta_pct:.1%} Downside)"
                self.lbl_share_price.setText(f"Implied Price: <span style='color:{COLOR_GREEN}'>${implied:,.2f}</span>  |  Market Price: ${current_price:,.2f}  <span style='color:{color}'>{delta_str}</span>")
            else:
                self.lbl_share_price.setText(f"Implied Price: <span style='color:{COLOR_GREEN}'>${implied:,.2f}</span>  |  Market Price: N/A")

            self.populate_main_table(self.dcf_model)
            self.populate_sens_table(self.valuation_data["sensitivity_matrix"])
            
            self.dupont_model = self.pipeline.build_dupont(self.clean_financials)
            self.populate_dupont_table(self.dupont_model)
            
            peers = self.input_peers.text()
            self.comps_model = self.pipeline.build_comps(ticker, peers)
            self.populate_comps_table(self.comps_model)
            
            self.btn_export.setEnabled(True)
            self.update_tabs_visibility()
            
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def populate_main_table(self, df):
        self.table_main.setRowCount(df.shape[0])
        self.table_main.setColumnCount(df.shape[1])
        self.table_main.setHorizontalHeaderLabels([str(x) for x in df.columns])
        self.table_main.setVerticalHeaderLabels(df.index)
        hist_cols = df.shape[1] - 5
        comp_mode = self.chk_compare.isChecked() and self.ref_dcf is not None
        for row in range(df.shape[0]):
            is_pct_row = "%)" in df.index[row]
            for col in range(df.shape[1]):
                val = df.iloc[row, col]
                display_val = f"${val:,.0f}" if abs(val) > 1000 else f"{val:.2%}"
                item = QTableWidgetItem(display_val)
                if comp_mode:
                    try: old_val = self.ref_dcf.iloc[row, col]
                    except: old_val = val
                    if pd.isna(val) or np.isinf(val): val = 0.0
                    if pd.isna(old_val) or np.isinf(old_val): old_val = 0.0
                    diff = val - old_val
                    if diff > 0: item.setForeground(QBrush(QColor(COLOR_GREEN)))
                    elif diff < 0: item.setForeground(QBrush(QColor(COLOR_RED)))
                    else: item.setForeground(QBrush(QColor(self.color_text)))
                    diff_pct = diff / abs(old_val) if old_val != 0 else 0.0
                    if is_pct_row: item.setToolTip(f"Diff: {diff:+.2%} (Abs)")
                    else: item.setToolTip(f"Diff: ${diff:+,.2f} ({diff_pct:+.2%})")
                else:
                    if col < hist_cols: item.setForeground(QBrush(QColor(self.color_override))) 
                    else: item.setForeground(QBrush(QColor(self.color_text))) 
                self.table_main.setItem(row, col, item)
        self.table_main.resizeColumnsToContents()

    def populate_sens_table(self, df):
        self.table_sens.setRowCount(df.shape[0])
        self.table_sens.setColumnCount(df.shape[1])
        self.table_sens.setHorizontalHeaderLabels([f"{col:.1%}" for col in df.columns])
        self.table_sens.setVerticalHeaderLabels([f"{row:.1%}" for row in df.index])
        comp_mode = self.chk_compare.isChecked() and self.ref_sens is not None
        for row in range(df.shape[0]):
            for col in range(df.shape[1]):
                val = df.iloc[row, col]
                item = QTableWidgetItem(f"${val:,.2f}")
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                if comp_mode:
                    try: old_val = self.ref_sens.iloc[row, col]
                    except: old_val = val
                    if pd.isna(val) or np.isinf(val): val = 0.0
                    if pd.isna(old_val) or np.isinf(old_val): old_val = 0.0
                    diff = val - old_val
                    if diff > 0: item.setForeground(QBrush(QColor(COLOR_GREEN)))
                    elif diff < 0: item.setForeground(QBrush(QColor(COLOR_RED)))
                    else: item.setForeground(QBrush(QColor(self.color_text)))
                    diff_pct = diff / abs(old_val) if old_val != 0 else 0.0
                    item.setToolTip(f"Diff: ${diff:+,.2f} ({diff_pct:+.2%})")
                else:
                    item.setForeground(QBrush(QColor(self.color_text))) 
                self.table_sens.setItem(row, col, item)
        self.table_sens.resizeColumnsToContents()

    def populate_dupont_table(self, df):
        self.table_dupont.setRowCount(df.shape[0])
        self.table_dupont.setColumnCount(df.shape[1])
        self.table_dupont.setHorizontalHeaderLabels([str(x) for x in df.columns])
        self.table_dupont.setVerticalHeaderLabels(df.index)
        for row in range(df.shape[0]):
            row_name = df.index[row]
            is_pct = "%)" in row_name
            is_mult = "(x)" in row_name
            for col in range(df.shape[1]):
                val = df.iloc[row, col]
                if pd.isna(val) or np.isinf(val): val = 0.0
                if is_pct: display_val = f"{val:.2%}"
                elif is_mult: display_val = f"{val:.2f}x"
                else: display_val = f"${val:,.0f}"
                item = QTableWidgetItem(display_val)
                item.setForeground(QBrush(QColor(self.color_override))) 
                self.table_dupont.setItem(row, col, item)
        self.table_dupont.resizeColumnsToContents()

    def populate_comps_table(self, df):
        self.table_comps.setRowCount(df.shape[0])
        self.table_comps.setColumnCount(df.shape[1])
        self.table_comps.setHorizontalHeaderLabels([str(x) for x in df.columns])
        self.table_comps.setVerticalHeaderLabels(df.index)
        for row in range(df.shape[0]):
            row_name = df.index[row]
            is_pct = "%)" in row_name
            is_price = "Price" in row_name
            for col in range(df.shape[1]):
                val = df.iloc[row, col]
                if pd.isna(val) or np.isinf(val): val = 0.0
                if is_pct: display_val = f"{val:.2%}"
                elif is_price: display_val = f"${val:,.2f}"
                else: display_val = f"{val:.2f}x"
                item = QTableWidgetItem(display_val)
                item.setForeground(QBrush(QColor(self.color_text))) 
                self.table_comps.setItem(row, col, item)
        self.table_comps.resizeColumnsToContents()

    def export_excel(self):
        ticker = self.input_ticker.text().upper() 
        timestamp = datetime.now().strftime("%m%d%y_%H%M")
        filename = f"{ticker}_{timestamp}.xlsx"
        
        export_routing = {
            'sens': self.routing_chks['sens']['excel'].isChecked(),
            'dupont': self.routing_chks['dupont']['excel'].isChecked(),
            'comps': self.routing_chks['comps']['excel'].isChecked()
        }
        
        current_price = self.current_info.get("currentPrice", self.current_info.get("regularMarketPrice", 0))
        dupont_model = getattr(self, 'dupont_model', None)
        comps_model = getattr(self, 'comps_model', None)
        
        if self.chk_compare.isChecked() and self.ref_dcf is not None:
            self.pipeline.export_live_excel(ticker, self.clean_financials, self.dcf_model, self.valuation_data, self.get_assumptions(), filename, export_routing, current_price, dupont_model, comps_model, self.ref_dcf, self.ref_assump, self.ref_sens)
        else:
            self.pipeline.export_live_excel(ticker, self.clean_financials, self.dcf_model, self.valuation_data, self.get_assumptions(), filename, export_routing, current_price, dupont_model, comps_model)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    window = DCFDashboard()
    window.show()
    sys.exit(app.exec())