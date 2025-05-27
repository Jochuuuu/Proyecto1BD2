# Base de Datos con Técnicas de Indexación

## Introducción

Este proyecto implementa un sistema de gestión de bases de datos que utiliza diferentes técnicas de indexación para optimizar la gestión, almacenamiento y recuperación de datos. El sistema está desarrollado en **Python** y permite comparar la eficiencia de distintas estructuras de datos para operaciones de base de datos.

## Objetivos

**Implementar y comparar técnicas de indexación:**

- **B+ Tree**: Estructura balanceada optimizada para sistemas de archivos  
- **AVL Tree**: Árbol binario auto-balanceado  
- **Extendible Hashing**: Técnica de hashing dinámico

**Operaciones implementadas para cada técnica:**

- Búsqueda específica (`Search`)
- Búsqueda por rango (`Range Search`)
- Inserción de registros (`Insert`)
- Eliminación de registros (`Remove`)

**Análisis de rendimiento:**

- Comparación de tiempos de ejecución  
- Evaluación de eficiencia en diferentes volúmenes de datos  
- Medición de complejidad temporal y espacial

## Técnicas de Indexación Implementadas

### 1. B+ Tree (Unclustered)

Estructura de datos balanceada especialmente diseñada para sistemas de almacenamiento que requieren acceso secuencial eficiente y operaciones de rango optimizadas.

**Características:**

- Todos los datos se almacenan en las hojas  
- Nodos internos solo contienen claves de navegación  
- Hojas enlazadas para facilitar recorridos secuenciales  
- **Complejidad**: `O(log_m n)` para búsqueda, inserción y eliminación

### 2. AVL Tree

Árbol binario de búsqueda auto-balanceado que mantiene la diferencia de alturas entre subárboles en máximo 1.

**Características:**

- Balanceo automático mediante rotaciones  
- Búsquedas eficientes en tiempo logarítmico  
- Ideal para aplicaciones con muchas operaciones de búsqueda  
- **Complejidad**: `O(lg n)` para todas las operaciones básicas

### 3. Extendible Hashing

Técnica de hashing dinámico que permite el crecimiento incremental del espacio de direcciones.

**Características:**

- Directorio de buckets que puede duplicarse dinámicamente  
- No requiere reorganización completa al crecer  
- Eficiente para grandes volúmenes de datos  
- **Complejidad**: `O(1)` promedio para búsqueda e inserción 

## Estructura del Proyecto

```
BD2/
├── main.py                 # API FastAPI - Punto de entrada principal
├── sql.py                  # Gestor SQL - Parser y ejecución de comandos
├── tabla.py                # Gestor de almacenamiento de tablas
├── README.md              # Documentación del proyecto
│
├── estructuras/           # Estructuras de datos e indexación
│   ├── avl.py            # Implementación del AVL Tree
│   ├── btree.py          # Implementación del B+ Tree
│   ├── hash.py           # Implementación del Extendible Hashing
│   └── point_class.py    # Clase Point para manejo de coordenadas 2D
│
├── tablas/               # Directorio de archivos de datos
│   ├── *.bin            # Archivos binarios de tablas
│   └── *_meta.json      # Metadatos de tablas
│
└── indices/              # Directorio de archivos de índices
    ├── *_avl.dat        # Índices AVL
    ├── *_tree.dat       # Índices B+ Tree
    ├── *_meta.dat       # Metadatos de índices B+ Tree
    ├── *_index.dat      # Índices Hash - Directorio
    └── *_buckets.dat    # Índices Hash - Buckets
```

## Componentes Principales

### 1. **API REST (main.py)**
- Servidor FastAPI que expone endpoints HTTP
- Ejecuta comandos SQL a través de requests POST
- Soporta CORS para integración con frontend
- Manejo de serialización de tipos especiales (Point)

### 2. **Parser SQL (sql.py)**
- Interpreta comandos SQL estándar: CREATE, INSERT, SELECT, DELETE
- Soporte para importación desde CSV
- Manejo de tipos de datos: INT, DECIMAL, VARCHAR, BOOL, DATE, POINT
- Procesamiento de condiciones WHERE con rangos y comparaciones

### 3. **Gestor de Tablas (tabla.py)**
- Almacenamiento eficiente en archivos binarios
- Lista libre para reutilización de registros eliminados
- Gestión automática de índices múltiples por tabla
- Validación y conversión de tipos de datos

### 4. **Estructuras de Indexación**

#### **AVL Tree (avl.py)**
- Árbol binario auto-balanceado
- Rotaciones automáticas para mantener balance
- Soporte para claves duplicadas y únicas
- Búsquedas exactas y por rango

#### **B+ Tree (btree.py)**
- Estructura optimizada para almacenamiento en disco
- Hojas enlazadas para recorridos secuenciales
- Persistencia automática en archivos .dat
- Soporte completo para rangos

#### **Extendible Hashing (hash.py)**
- Hashing dinámico con crecimiento incremental
- Manejo de overflow con buckets enlazados
- Ideal para búsquedas exactas O(1)
- No soporta búsquedas por rango

### 5. **Tipo de Dato POINT (point_class.py)**
- Representa coordenadas 2D (x, y)
- Operaciones matemáticas sobrecargadas
- Comparaciones basadas en distancia al origen
- Búsquedas rectangulares para rangos

## Tipos de Datos Soportados

| Tipo | Descripción | Ejemplo |
|------|-------------|---------|
| `INT` | Números enteros | `42` |
| `DECIMAL` | Números decimales | `3.14159` |
| `VARCHAR[n]` | Cadenas de longitud variable | `'Texto'` |
| `CHAR[n]` | Cadenas de longitud fija | `'ABC'` |
| `BOOL` | Valores booleanos | `true`, `false` |
| `DATE` | Fechas como timestamp | `1640995200` |
| `POINT` | Coordenadas 2D | `(1.5, 2.3)` |

## Comandos SQL Soportados

### CREATE TABLE
```sql
CREATE TABLE Productos (
    id INT KEY INDEX btree,
    nombre VARCHAR[50] INDEX avl,
    precio DECIMAL INDEX hash,
    ubicacion POINT INDEX btree
);
```

### INSERT
```sql
INSERT INTO Productos VALUES (1, 'Laptop', 999.99, '(10.5, 20.3)');
```

### SELECT
```sql
-- Búsqueda exacta
SELECT * FROM Productos WHERE id = 1;

-- Búsqueda por rango
SELECT nombre, precio FROM Productos WHERE precio BETWEEN 100 AND 1000;

-- Búsqueda de puntos en área rectangular
SELECT * FROM Productos WHERE ubicacion BETWEEN '(0, 0)' AND '(50, 50)';
```

### DELETE
```sql
DELETE FROM Productos WHERE precio > 500;
```

### IMPORT FROM CSV
```sql
IMPORT FROM CSV 'datos.csv' INTO Productos;
```

## Instalación y Uso

### Requisitos
- Python 3.8+
- FastAPI
- Uvicorn
- Pydantic

### Ejemplo de Request
```json POST /sql
{
    "sql": "CREATE TABLE Empleados (id INT KEY INDEX btree, nombre VARCHAR[100] INDEX avl, salario DECIMAL INDEX hash);"
}
```

## Características Especiales

###  **Manejo Avanzado de Tipos**
- Conversión automática de tipos en CSV
- Serialización JSON de objetos Point
- Validación estricta de tipos de datos

### **Optimización de Consultas**
- Intersección eficiente de múltiples condiciones
- Optimización de búsquedas por rango

###  **Persistencia Robusta**
- Archivos binarios optimizados
- Metadatos JSON para estructura
- Recovery automático de índices

### **Análisis de Rendimiento**
- Tiempos de ejecución por operación
- Estadísticas de uso de índices
- Métricas de almacenamiento

## Comparación de Técnicas

| Operación | AVL Tree | B+ Tree | Hash |
|-----------|----------|---------|------|
| **Búsqueda Exacta** | O(log n) | O(log n) | O(1) |
| **Búsqueda Rango** | O(log n + k) | O(log n + k) | ❌ |
| **Inserción** | O(log n) | O(log n) | O(1) |
| **Eliminación** | O(log n) | O(log n) | O(1) |
| **Espacio** | Medio | Alto | Bajo |
| **Uso Recomendado** | Búsquedas mixtas | Rangos frecuentes | Búsquedas exactas |
