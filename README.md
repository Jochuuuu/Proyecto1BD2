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

---

## Estructura del Proyecto

Proyecto/
├── estructuras/
│   ├── btree.py  
│   ├── avl_tree.py  
│   ├── point_class.py 
│   └── hash.py  
├── indices/
│   #guarda los indices creados por las estructuras en .dat
├── tablas/
│   #guarda la tabla creada en .bin 
├── frontend/
│   #frontend
├── tabla.py  
├── sql.py  
└── main.py 




