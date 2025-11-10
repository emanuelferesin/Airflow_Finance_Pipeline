# 📈 Airflow-Finance-SCD2-Pipeline

![CI Pipeline](https://github.com/emanuelferesin/Airflow_Finance_Pipeline/actions/workflows/ci.yml/badge.svg)

Este proyecto implementa un **Pipeline de Ingeniería de Datos ELT** de extremo a extremo, orquestado con **Apache Airflow 2.x** y desplegado usando **Docker Compose**. El objetivo principal es **consolidar datos financieros temporales** de múltiples APIs para construir un Data Warehouse en **Redshift**, utilizando un **Esquema Estrella** avanzado.

### 🔑 Características Destacadas

* **Arquitectura ELT:** Realiza una carga inicial de datos crudos a una tabla de *staging* y utiliza SQL en Redshift para las transformaciones posteriores (L-T).
* **Múltiples Fuentes de Datos Temporales (3 APIs):** Consolida series de tiempo de precios de acciones, metadatos de seguridad y tasas de cambio de dólar para generar métricas derivadas.
* **Modelado Dimensional Avanzado:** Implementación de un **Esquema Estrella** que incluye:
    * **Tabla de Hechos:** `fact_prices` (Métricas de precios en USD y Moneda Local).
    * **Tabla de Dimensión:** `dim_security` (Atributos de la acción, usando lógica **SCD Tipo 2** para rastrear el historial de cambios).
* **Ingeniería de Código:** Uso de código Python modular (`src/`) con **Type Hinting** y pruebas unitarias (`pytest`).
* **Integración Continua (CI):** Flujo de trabajo automatizado con **GitHub Actions** para correr los tests unitarios en cada Pull Request.

---

### 2. 🚫 Contenido para `.gitignore` (Archivo Oculto)

Abre y pega esto. Es fundamental para la seguridad del proyecto.
