# 🚀 Trabajo Integrador — IA en Producción

Plataforma para pronóstico de producción de hidrocarburos utilizando Machine Learning en un entorno productivo.

---

## 📌 Overview

Este proyecto tiene como objetivo desarrollar un **pipeline de ML listo para producción** que permita:

- Predecir la producción futura de hidrocarburos
- Reducir la incertidumbre operativa
- Exponer resultados vía API REST
- Garantizar trazabilidad y reproducibilidad de modelos

> ⚠️ **Nota:** El foco está en **ML Engineering**, no en la precisión del modelo.

---

## 🧠 Problem Statement

Actualmente, los equipos enfrentan:

- Alta incertidumbre en planificación
- Procesos manuales y dispersos
- Baja trazabilidad de decisiones
- Dependencia del conocimiento individual

---

## 🎯 Objectives

- 📈 **Forecasting:** Predicción a corto, medio y largo plazo  
- 🔍 **Reducir incertidumbre operativa**  
- 🔗 **Integración vía API REST**  
- 🧾 **Trazabilidad de modelos y datos**

---

## 🏗️ Architecture

```mermaid
flowchart LR
    DW[Data Warehouse] --> ORQ[Orquestación + Retrain]
    ORQ --> PRE[Preprocessing]
    PRE --> FS[Feature Store]
    FS --> TR[Training]
    TR --> VAL[Validation]
    VAL --> MR[Model Registry]
    MR --> API[API Service]
    API --> USERS[API Users]

    TR --> EXP[Experiment Tracking]
    API --> OBS[Model Observability]
```

---

## 👥 Users

### 🔌 API Consumers
- Consumen predicciones vía REST

### 🧪 ML Engineers
- Acceden a experiment tracking
- Gestionan entrenamiento y despliegue

---

## 📊 Datasets

- Producción de pozos  
- Listado de pozos  

---

## 🔌 API Specification

### Base URL
```
/api/v1
```

### 📍 `GET /forecast`

Obtiene el pronóstico de producción.

#### Query Params
| Param        | Type   | Required | Description              |
|--------------|--------|----------|--------------------------|
| id_well      | string | ✅       | ID del pozo              |
| date_start   | date   | ✅       | Fecha inicio             |
| date_end     | date   | ✅       | Fecha fin                |

#### Response
```json
{
  "id_well": "POZO-001",
  "data": [
    {
      "date": "2023-10-01",
      "prod": 150.5
    }
  ]
}
```

---

### 📍 `GET /wells`

Listado de pozos disponibles.

#### Query Params
| Param       | Type | Required | Description |
|-------------|------|----------|------------|
| date_query  | date | ✅       | Fecha de consulta |

#### Response
```json
[
  {
    "id_well": "POZO-001"
  }
]
```

---

## ⚙️ Tech Requirements

- Docker & Docker Compose
- API REST
- Feature Store
- Model Registry
- Experiment Tracking
- Orquestación

---

## 🚀 Getting Started

### 1. Clonar repo
```bash
git clone <repo-url>
cd <repo>
```

### 2. Levantar entorno
```bash
docker-compose up --build
```

### 3. Acceder a servicios

- API → http://localhost:8000
- Docs → http://localhost:8000/docs

---

## 🔁 ML Pipeline

- Ingesta de datos
- Feature engineering → Feature Store
- Training reproducible
- Logging de métricas y artefactos
- Registro de modelos
- Serving vía API

---

## 📦 Deliverables

### 📅 Entrega Parcial

- [ ] Sistema dockerizado
- [ ] API funcional
- [ ] Experiment tracking
- [ ] Logging de métricas
- [ ] Feature Store persistente
- [ ] Training reproducible

---

### 📅 Entrega Final

- [ ] Orquestación automática
- [ ] Retraining periódico
- [ ] Model decay
- [ ] Data drift / concept drift
- [ ] Infraestructura escalable

---

## 🤝 Collaboration Guidelines

- Uso obligatorio de Pull Requests
- Commits claros y distribuidos
- Trabajo equitativo entre miembros
