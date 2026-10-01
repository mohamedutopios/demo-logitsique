"""IA : prévision du nombre de commandes par jour, site et client, sur les 7 prochains jours.

Modèle : gradient boosting (scikit-learn) sur le jour de la semaine et les volumes passés
(J-7, J-14, moyenne des 7 derniers jours). Il est comparé à une référence simple :
« même jour la semaine précédente ». Résultat écrit dans ia.previsions_commandes.

Utilisation : python services/prevoir_volumes.py
"""
import os
from datetime import timedelta
import pandas as pd
import psycopg
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error

DWH_DSN = os.environ.get("DWH_ADMIN_DSN", "postgresql://dwh_admin:dwh_admin@dwh:5432/dwh")
HORIZON, VALIDATION = 7, 14


def variables(serie):
    df = pd.DataFrame({"y": serie})
    df["jour_semaine"] = df.index.dayofweek
    df["j_7"] = df["y"].shift(7)
    df["j_14"] = df["y"].shift(14)
    df["moy_7"] = df["y"].shift(1).rolling(7).mean()
    return df


COLONNES = ["jour_semaine", "j_7", "j_14", "moy_7"]


def main():
    with psycopg.connect(DWH_DSN) as cx:
        with cx.cursor() as cur:
            cur.execute("""SELECT jour, site, code_client, nb_commandes_creees FROM marts.mart_logistique_jour
                           WHERE jour < current_date ORDER BY jour""")
            data = pd.DataFrame(cur.fetchall(), columns=["jour", "site", "code_client", "n"])
        data["jour"] = pd.to_datetime(data["jour"])
        resultats, bilan = [], []
        for (site, client), g in data.groupby(["site", "code_client"]):
            serie = g.set_index("jour")["n"].asfreq("D", fill_value=0).astype(float)
            df = variables(serie).dropna()
            if len(df) < VALIDATION + 14:
                continue
            app, val = df.iloc[:-VALIDATION], df.iloc[-VALIDATION:]
            modele = GradientBoostingRegressor(n_estimators=200, max_depth=3, learning_rate=0.05, random_state=42)
            modele.fit(app[COLONNES], app["y"])
            mae_modele = mean_absolute_error(val["y"], modele.predict(val[COLONNES]))
            mae_reference = mean_absolute_error(val["y"], val["j_7"])
            modele.fit(df[COLONNES], df["y"])
            histo = serie.copy()
            for h in range(1, HORIZON + 1):
                jour = histo.index[-1] + timedelta(days=1)
                x = pd.DataFrame([{"jour_semaine": jour.dayofweek, "j_7": histo.iloc[-7], "j_14": histo.iloc[-14],
                                   "moy_7": histo.iloc[-7:].mean()}])
                pred = max(0.0, float(modele.predict(x)[0]))
                histo.loc[jour] = pred
                resultats.append((jour.date(), site, client, round(pred, 1), round(mae_modele, 2), round(mae_reference, 2)))
            bilan.append((site, client, mae_modele, mae_reference))
        with cx.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS ia.previsions_commandes (
                             jour date, site text, code_client text, commandes_prevues numeric,
                             mae_modele numeric, mae_reference numeric, genere_le timestamptz DEFAULT now(),
                             PRIMARY KEY (jour, site, code_client))""")
            cur.execute("DELETE FROM ia.previsions_commandes WHERE jour >= current_date")
            cur.executemany("""INSERT INTO ia.previsions_commandes (jour, site, code_client, commandes_prevues, mae_modele, mae_reference)
                               VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (jour, site, code_client) DO UPDATE
                               SET commandes_prevues = EXCLUDED.commandes_prevues, mae_modele = EXCLUDED.mae_modele,
                                   mae_reference = EXCLUDED.mae_reference, genere_le = now()""", resultats)
        cx.commit()
    print(f"{'site':<5}{'client':<8}{'MAE modèle':>12}{'MAE J-7':>10}")
    for site, client, m, r in bilan:
        print(f"{site:<5}{client:<8}{m:>12.1f}{r:>10.1f}{'   ← le modèle bat la référence' if m < r else ''}")
    print(f"{len(resultats)} prévisions écrites dans ia.previsions_commandes")


if __name__ == "__main__":
    main()
