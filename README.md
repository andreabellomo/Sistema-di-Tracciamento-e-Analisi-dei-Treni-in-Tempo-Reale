# TAP_project


Con le variabili fornite (categoria, numTreno, stazPart, oraPart, ritardoPart, stazArr, oraArr, ritardoArr, provvedimenti, variazioni), ci sono diversi task di machine learning che si possono applicare a seconda degli obiettivi che si vogliono raggiungere. Ecco alcune opzioni:

1. **Previsione del ritardo all'arrivo (Regression Task)**:
   - Obiettivo: Prevedere il ritardo di arrivo (ritardoArr) di un treno.
   - Algoritmi possibili: Regressione lineare, Random Forest Regressor, Gradient Boosting, Reti Neurali, etc.
   - Variabili utilizzate: categoria, numTreno, stazPart, oraPart, ritardoPart, stazArr, oraArr, provvedimenti, variazioni.

2. **Classificazione del ritardo (Classification Task)**:
   - Obiettivo: Classificare il ritardo di arrivo in diverse categorie (es. "in orario", "ritardo lieve", "ritardo grave").
   - Algoritmi possibili: Random Forest Classifier, Support Vector Machine, Reti Neurali, K-Nearest Neighbors, etc.
   - Variabili utilizzate: categoria, numTreno, stazPart, oraPart, ritardoPart, stazArr, oraArr, provvedimenti, variazioni.

3. **Rilevazione di anomalie nei ritardi (Anomaly Detection)**:
   - Obiettivo: Identificare ritardi anomali che non seguono il pattern usuale.
   - Algoritmi possibili: Isolation Forest, Local Outlier Factor, One-Class SVM, etc.
   - Variabili utilizzate: categoria, numTreno, stazPart, oraPart, ritardoPart, stazArr, oraArr, provvedimenti, variazioni.

4. **Previsione della durata del viaggio (Regression Task)**:
   - Obiettivo: Prevedere la durata totale del viaggio da stazPart a stazArr.
   - Algoritmi possibili: Regressione lineare, Random Forest Regressor, Gradient Boosting, Reti Neurali, etc.
   - Variabili utilizzate: categoria, numTreno, stazPart, oraPart, stazArr, oraArr, provvedimenti, variazioni.

5. **Analisi dei fattori di ritardo (Exploratory Data Analysis - EDA)**:
   - Obiettivo: Analizzare i fattori che contribuiscono maggiormente ai ritardi.
   - Tecniche possibili: Analisi di correlazione, visualizzazione dei dati, clustering per gruppi di ritardi simili, etc.
   - Variabili utilizzate: tutte.

6. **Ottimizzazione della gestione del traffico ferroviario (Optimization Task)**:
   - Obiettivo: Ottimizzare i provvedimenti e le variazioni per minimizzare i ritardi complessivi.
   - Algoritmi possibili: Programmazione Lineare, Algoritmi Evolutivi, Reinforcement Learning, etc.
   - Variabili utilizzate: categoria, numTreno, stazPart, oraPart, ritardoPart, stazArr, oraArr, ritardoArr, provvedimenti, variazioni.

A seconda del task specifico scelto, le tecniche di pre-processing dei dati, la selezione delle feature e la valutazione delle performance del modello varieranno.
