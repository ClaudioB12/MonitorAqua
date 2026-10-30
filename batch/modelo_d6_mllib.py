"""
F6-D6 con Spark MLlib: la MISMA regresión logística de batch/fuga.py entrenada con LogisticRegression de MLlib.

MLlib es el entrenamiento distribuido (escala a la flota completa y a más corridas sin cambiar el código); numpy
(batch/fuga.py) es la inferencia ligera que corre en F7 sin Spark. Este script comprueba que los dos dan el mismo
modelo: mismas noches (batch/datos/d6_noches_modelo.parquet, que escribe batch/modelo_d6.py), mismas variables
estandarizadas con la media y la desviación del modelo numpy, mismos pesos de clase y la misma penalización L2.

  Equivalencia de la L2: numpy maximiza Σ w·loglik − (l2/2)·||β||²; MLlib minimiza (Σ w·pérdida)/Σw + (regParam/2)·||β||²
  -> regParam = l2 / Σw (sin estandarizar dentro de MLlib: standardization=False, porque ya entran estandarizadas).

  MSYS_NO_PATHCONV=1 docker run --rm -v "$PWD:/repo" -w /repo -e PYTHONPATH=/repo/batch monitoraqua-streaming \\
      spark-submit --driver-memory 2g batch/modelo_d6_mllib.py
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

RAIZ = Path(__file__).resolve().parents[1]
CFG = yaml.safe_load((RAIZ / "batch" / "modelo_d6.yml").read_text(encoding="utf-8"))


def metricas(d, marca):
    fuga = (d["etiqueta"] == "fuga_lenta").to_numpy()
    toca = (d["frac_fuga_lenta"] > 0).to_numpy()
    eps = d.loc[fuga, "episodio_fuga"].dropna().unique()
    det = sum(1 for ep in eps if marca[(d["episodio_fuga"] == ep).to_numpy() & fuga].any())
    return {"episodios": int(len(eps)), "recall_episodios": round(det / len(eps), 3) if len(eps) else None,
            "recall_noches": round(int((marca & fuga).sum()) / max(1, int(fuga.sum())), 3),
            "precision_noches": round(int((marca & toca).sum()) / max(1, int(marca.sum())), 3),
            "noches_marcadas": int(marca.sum())}


def main():
    version = CFG["version"]
    destino = sorted((RAIZ / "batch" / "modelos" / "d6").glob(f"{version}-*"))[-1]
    m = json.loads((destino / "modelo.json").read_text(encoding="utf-8"))["clasificador"]
    variables, media, desv = m["variables"], np.array(m["media"]), np.array(m["desv"])
    n = pd.read_parquet(RAIZ / "batch" / "datos" / "d6_noches_modelo.parquet")
    n = n[n["evaluable"]].copy()
    Z = (n[variables].astype(float).fillna(0.0).to_numpy() - media) / desv
    n["y"] = (n["etiqueta"] == "fuga_lenta").astype(float)
    ent = n["conj"] == "entrenamiento"
    pos = n.loc[ent, "y"].mean()
    n["w"] = np.where(n["y"] == 1, 0.5 / pos, 0.5 / (1 - pos))

    spark = SparkSession.builder.master("local[2]").appName("f6-d6-mllib").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    filas = [(Vectors.dense(z), float(y), float(w)) for z, y, w in zip(Z[ent.to_numpy()], n.loc[ent, "y"], n.loc[ent, "w"])]
    df = spark.createDataFrame(filas, ["features", "label", "weight"])
    reg = CFG["modelo"]["l2"] / float(n.loc[ent, "w"].sum())
    lr = LogisticRegression(featuresCol="features", labelCol="label", weightCol="weight", regParam=reg,
                            elasticNetParam=0.0, standardization=False, fitIntercept=True, maxIter=200, tol=1e-10)
    modelo = lr.fit(df)
    beta_mllib = np.r_[modelo.intercept, modelo.coefficients.toArray()]
    beta_numpy = np.array(m["beta"])

    # Probabilidades de MLlib (misma fórmula logística con sus coeficientes) en todas las noches evaluables
    n["p_mllib"] = 1 / (1 + np.exp(-(beta_mllib[0] + Z @ beta_mllib[1:])))
    umbral = m["umbral"]
    out = {"que_es": "misma regresión logística entrenada con Spark MLlib (distribuido) y con numpy (inferencia en F7)",
           "regParam_mllib": reg, "l2_numpy": CFG["modelo"]["l2"], "noches_entrenamiento": int(ent.sum()),
           "iteraciones_mllib": int(modelo.summary.totalIterations),
           "coeficientes": {v: {"numpy": round(float(a), 4), "mllib": round(float(b), 4)}
                            for v, a, b in zip(["intercepto"] + variables, beta_numpy, beta_mllib)},
           "max_dif_coeficiente": float(np.max(np.abs(beta_numpy - beta_mllib))),
           "max_dif_probabilidad": float(np.max(np.abs(n["p_mllib"] - n["p_fuga"]))),
           "umbral": umbral, "evaluacion": {}}
    for fuente in ("aumentado_f2b2", "aumentado_f2b3"):
        d = n[(n["fuente"] == fuente) & (n["conj"] == "evaluacion_consistencia")]
        out["evaluacion"][fuente] = {"numpy": metricas(d, (d["p_fuga"] >= umbral).to_numpy()),
                                     "mllib": metricas(d, (d["p_mllib"] >= umbral).to_numpy()),
                                     "noches_con_decision_distinta": int(((d["p_fuga"] >= umbral) != (d["p_mllib"] >= umbral)).sum())}
    evid = RAIZ / "docs" / "evidencias" / "f6-d6" / "mllib_vs_numpy.json"
    evid.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"service": "f6-d6-mllib", "status": "listo", "max_dif_coeficiente": out["max_dif_coeficiente"],
                      "max_dif_probabilidad": out["max_dif_probabilidad"]}), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
