# 📈 Airflow-Finance-SCD2-Pipeline

![CI Pipeline](https://github.com/emanuelferesin/Airflow_Finance_Pipeline/actions/workflows/ci.yml/badge.svg)

Este proyecto implementa un **Pipeline de Ingeniería de Datos ELT** de extremo a extremo, orquestado con **Apache Airflow 2.x** y desplegado usando **Docker Compose**. El objetivo principal es **consolidar datos financieros temporales** de múltiples APIs para construir un Data Warehouse en **PostgreSQL**, utilizando un **Esquema Estrella** avanzado.

---

## 📊 Estado del Proyecto

✅ **Días 1-2**: Setup y Docker Infrastructure  
✅ **Días 3-4**: Extracción y Transformación de Datos  
✅ **Día 5**: Testing Unitario (7 tests passing)  
✅ **Día 6**: CI/CD con GitHub Actions  
🔄 **Próximo**: DAG de Airflow y Modelado Dimensional  

---

## 🔑 Características Destacadas

* **Arquitectura ELT:** Realiza una carga inicial de datos crudos a una tabla de *staging* y utiliza SQL en PostgreSQL para las transformaciones posteriores (L-T).
* **Múltiples Fuentes de Datos Temporales (3 APIs):** Consolida series de tiempo de precios, tipos de cambio y datos adicionales para generar métricas derivadas.
* **Histórico Configurable:** Parámetro `dias_historico` permite generar datos desde 1 hasta 365 días para backfill y testing.
* **Modelado Dimensional Avanzado:** Implementación de un **Esquema Estrella** que incluye:
    * **Tabla de Hechos:** `fact_ventas` (Métricas de precios en USD y Moneda Local).
    * **Tabla de Dimensión:** `dim_producto` (Atributos del producto, usando lógica **SCD Tipo 2** para rastrear el historial de cambios).
* **Ingeniería de Código:** Uso de código Python modular (`src/`) con **Type Hinting** y pruebas unitarias (`pytest`).
* **Integración Continua (CI):** Flujo de trabajo automatizado con **GitHub Actions** para correr los tests unitarios en cada commit.

---

## 🏗️ Arquitectura del Proyecto
```
APIs Externas → Airflow DAG → PostgreSQL (Staging) → Transformación SQL → Data Warehouse
                    ↓
              Testing (pytest)
                    ↓
           CI/CD (GitHub Actions)
```

---

## 🚀 Inicio Rápido

### Prerequisitos
- Docker Desktop
- Docker Compose v3.8+
- Git

### Instalación
```bash
# Clonar repositorio
git clone https://github.com/emanuelferesin/Airflow_Finance_Pipeline.git
cd Airflow_Finance_Pipeline

# Configurar variables de entorno
cp env_example .env

# Dar permisos a carpeta logs
chmod -R 777 logs/

# Iniciar servicios
docker-compose up -d

# Acceder a Airflow UI
# http://localhost:8080
# Usuario: admin
# Contraseña: admin
```

### Ejecutar Tests
```bash
# Dentro del contenedor
docker-compose exec webserver bash
pytest tests/ -v

# Salir del contenedor
exit
```

---

## 📁 Estructura del Proyecto
```
Airflow_Finance_Pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml              # CI/CD pipeline
├── dags/
│   └── __init__.py             # DAGs de Airflow (próximamente)
├── src/
│   ├── extractor.py            # Extracción de APIs con histórico
│   ├── transformer.py          # Transformación y consolidación
│   └── utils.py                # Utilidades y manejo de errores
├── tests/
│   ├── test_extractor.py       # Tests de extracción (4 tests)
│   └── test_transformer.py     # Tests de transformación (3 tests)
├── sql/
│   └── (próximamente)          # Scripts SQL para modelado
├── logs/                       # Logs de Airflow
├── plugins/                    # Plugins personalizados
├── docker-compose.yml          # Orquestación de servicios
├── Dockerfile                  # Imagen personalizada de Airflow
├── requirements.txt            # Dependencias Python
├── pytest.ini                  # Configuración de pytest
└── README.md
```

---

## 🧪 Testing

El proyecto incluye 7 tests unitarios que verifican la funcionalidad core:
```bash
# Ejecutar todos los tests
pytest tests/ -v

# Resultado esperado:
# 7 passed ✅
```

**Tests implementados:**
- ✅ Extracción de productos funciona correctamente
- ✅ Extracción tiene columnas requeridas
- ✅ Extracción de tipos de cambio funciona
- ✅ Datos de ARS están presentes
- ✅ Limpieza de datos elimina duplicados
- ✅ Cálculo de precio local es correcto
- ✅ Se agrega columna de moneda local

---

## 🔄 CI/CD

El proyecto utiliza GitHub Actions para:
- ✅ Ejecutar tests automáticamente en cada push
- ✅ Verificar calidad de código con flake8
- ✅ Validar instalación de dependencias

**Ver estado:** [![CI Pipeline](https://github.com/emanuelferesin/Airflow_Finance_Pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/emanuelferesin/Airflow_Finance_Pipeline/actions)

---

## 💾 Extracción de Datos

El módulo de extracción soporta histórico configurable:
```python
from src.extractor import extraer_todas_las_fuentes

# Extraer 7 días de histórico (default)
datos = extraer_todas_las_fuentes(dias_historico=7)

# Extraer 30 días para testing
datos = extraer_todas_las_fuentes(dias_historico=30)

# Para producción con Airflow
datos = extraer_todas_las_fuentes(dias_historico=1)
```

**Retorna:**
- DataFrame de productos (~161 productos × N días)
- DataFrame de tipos de cambio (~161 monedas × N días)
- DataFrame de datos adicionales (~10 registros × N días)

---

## 🔄 Transformación de Datos

El módulo de transformación consolida y calcula precios locales:
```python
from src.transformer import transformar_datos_completo

# Transformar y consolidar
df_final, resumen = transformar_datos_completo(
    datos['productos'],
    datos['tipos_cambio'],
    datos['adicionales'],
    moneda_local='ARS'
)

# Resultado: DataFrame consolidado con precio_local = precio_usd × tipo_cambio
```

---

## 📊 Modelo de Datos (Próximamente)

### Esquema Estrella

#### Tabla de Hechos: `fact_ventas`
- `venta_id` (PK)
- `producto_key` (FK)
- `fecha_key` (FK)
- `precio_usd`
- `precio_local`
- `tipo_cambio`

#### Dimensión: `dim_producto` (SCD Type 2)
- `producto_key` (PK - Surrogate Key)
- `producto_id` (Business Key)
- `nombre_producto`
- `categoria`
- `precio_base`
- `fecha_inicio`
- `fecha_fin`
- `es_actual`

---

## 🛠️ Tecnologías

- **Apache Airflow 2.7.2** - Orquestación
- **PostgreSQL 13** - Base de datos
- **Python 3.10** - Lenguaje principal
- **Docker & Docker Compose** - Containerización
- **Pandas** - Manipulación de datos
- **pytest** - Testing
- **GitHub Actions** - CI/CD

---

## 📝 Comandos Útiles
```bash
# Iniciar Airflow
docker-compose up -d

# Ver logs
docker-compose logs -f

# Detener Airflow
docker-compose down

# Limpiar todo (incluye base de datos)
docker-compose down -v

# Ejecutar tests
docker-compose exec webserver pytest tests/ -v

# Acceder al contenedor
docker-compose exec webserver bash
```

---

## 🤝 Contribución

1. Fork el proyecto
2. Crear rama feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit cambios (`git commit -m 'Add nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Crear Pull Request

---

## 📝 Licencia

Este proyecto es de código abierto bajo la licencia MIT.

---

## 👤 Autor

Emanuel Feresin - [@emanuelferesin](https://github.com/emanuelferesin)

---

## 🙏 Agradecimientos

- Apache Airflow Community
- Documentación de PostgreSQL
- GitHub Actions
- ExchangeRate API
- Coinbase API

---