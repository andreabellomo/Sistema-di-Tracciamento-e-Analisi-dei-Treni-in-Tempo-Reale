## **Progetto: STA-TR (Sistema di Tracciamento e Analisi dei Treni in Tempo Reale)**

### **Motivazione**
La necessità di migliorare l'affidabilità e l'efficienza del servizio ferroviario italiano è evidente. Attualmente, i dati aperti sui ritardi dei treni sono limitati e frammentati, rendendo difficile per i cittadini e le autorità monitorare le performance e intervenire per migliorare il servizio. Il nostro progetto, STA-TR, mira a colmare questa lacuna fornendo un sistema di monitoraggio e analisi in tempo reale del servizio ferroviario.

### **Obiettivi**
1. **Raccolta Dati**: Implementare un sistema per raccogliere dati sui treni in tempo reale da API esistenti.
2. **Analisi Dati**: Utilizzare modelli di regressione per prevedere i ritardi dei treni.
3. **Visualizzazione Dati**: Fornire una visualizzazione intuitiva dei dati raccolti e delle previsioni tramite Kibana.
4. **Trasparenza e Accountability**: Migliorare la trasparenza e la responsabilità delle compagnie ferroviarie italiane.

### **Architettura del Sistema & Data Flow**

![Architettura del Sistema](images\tap_flow.png)


1. **Logstash**:
   - Raccoglie dati da API che forniscono informazioni sui treni in tempo reale (esempio di dati: categoria, numTreno, stazPart, oraPart, ritardoPart, stazArr, oraArr, ritardoArr, provvedimenti, variazioni).
   
2. **Kafka**:
   - Trasmette i dati raccolti da Logstash a un cluster Kafka per garantire la scalabilità e la resilienza del sistema.

3. **Apache Spark**:
   - Legge i dati da Kafka e applica modelli di regressione lineare per prevedere i ritardi dei treni.

4. **Elasticsearch**:
   - Salva i dati analizzati e le previsioni per consentire ricerche rapide e complesse.

5. **Kibana**:
   - Visualizza i dati e le previsioni in dashboard interattive per consentire agli utenti di esplorare e comprendere i dati in modo intuitivo.

## 🏁 Installazione e Avvio </a>

Clone repo : 
```
git clone https://github.com/andreabellomo/TAP_project.git
```

Build spark image
```
cd spark
docker build -t spark_4.4.2 .
```
Nella cartella root : 
```
docker compose up
```

## Demo </a>
![Demo1](images\1.png)
![Demo2](images\2.png)
![Demo3](images\3.png)


## ✍️ Autore</a>

- Bellomo Andrea