# Задание 4.2. Стратегия горизонтального масштабирования

## Цель
Определить две наиболее критичные ИТ-системы компании и разработать для них план горизонтального масштабирования с учётом новой нагрузки мобильного приложения (50 000 пользователей в часы пик, отклик < 1 секунды для 99% запросов, доступность 99.95%).

## Выбранные критичные системы

| Система | Обоснование выбора |
|---------|-------------------|
| **Schedule Service (Расписание)** | Центральная бизнес-операция — запись к врачу. Монолитная БД PostgreSQL является узким местом при 50 000 пиковых пользователей. Текущая пропускная способность ~5 000 RPS падает при перегрузке. Без масштабирования целевой показатель 50 000 пользователей недостижим. |
| **API Gateway** | Единая точка входа для всех мобильных запросов. Текущая архитектура — одиночный инстанс с пропускной способностью ~5 000 RPS. При 50 000 пользователей это критический bottleneck. Горизонтальное масштабирование Gateway — prerequisite для масштабирования всех backend-сервисов. |

---

## Таблица горизонтального масштабирования

| ИТ-система | Реализация горизонтального масштабирования | Ограничение | Описание решения |
|------------|-------------------------------------------|-------------|-----------------|
| **Schedule Service (Расписание)** | **Database Sharding по doctor_id + Read Replicas** | **Ограничение:** Cross-shard запросы (например, поиск записей по patient_id по всем врачам) требуют scatter-gather по всем шардам, что увеличивает задержку и сложность. | **Решение:** 1. Основной сценарий (бронирование) — шардирование по doctor_id (знаем конкретного врача → один шард). 2. Поиск пациента — scatter-gather по всем шардам с кэшированием топ-10 врачей в Redis (30% запросов). 3. Read replicas на каждом шарде (1 Primary + 2 Replicas) — read/write split: чтения → replicas, записи → primary. 4. Consistent hashing по doctor_id для равномерного распределения. |
| **API Gateway** | **Stateless replicas + Load Balancer + Auto-scaling (Kubernetes HPA)** | **Ограничение:** Stateless-дизайн требует внешнего хранилища сессий. При переключении между репликами контекст пользователя (JWT, user context) должен быть доступен любой реплике. | **Решение:** 1. Kubernetes HPA: масштабирование при CPU >70% или RPS >8000. Min: 10 реплик (базовая нагрузка 500 пользователей), Max: 100 реплик (пик 50 000 пользователей). 2. NGINX/HAProxy как Load Balancer (L7 routing). 3. Redis Session Store: все реплики читают/пишут сессию из единого Redis Cluster. 4. JWT stateless verification: Gateway проверяет подпись JWT без обращения к Auth Service (подпись публичная). 5. Health checks каждые 10 секунд, remove из LB при 3 failure. |

---

## 1. Schedule Service: Database Sharding по doctor_id

### Текущее состояние

| Параметр | Значение |
|----------|----------|
| Архитектура | Монолит (1 инстанс, 1 БД PostgreSQL) |
| Запросов в пик | ~5 000 RPS |
| CPU | 95%+ (перегружен) |
| Latency (p99) | 10 секунд (или timeout) |
| RPS capacity | 5 000 RPS |

### Реализация горизонтального масштабирования

#### Стратегия: Database Sharding

```
Шардирование по doctor_id:

Шард 0:  Врачи ID 0-999
Шард 1:  Врачи ID 1000-1999
...
Шард N:  Врачи ID 2000+

Каждый шард:
  ├── 1 Primary (записи)
  ├── 1 Read Replica (чтения)
  └── 1 Read Replica (чтения)

Consistent Hashing:
  doctor_id → hash → шард
  Равномерное распределение нагрузки
```

#### Конфигурация

| Параметр | Значение |
|----------|----------|
| Количество шардов | 10-20 |
| Репликация | 1 Primary + 2 Read Replicas на шард |
| Ключ шардирования | doctor_id (врач = атомарная сущность) |
| Роутинг | Consistent hashing по doctor_id |
| Write/Read Split | Writes → Primary, Reads → Replicas |

#### Результаты масштабирования

| Метрика | До | После | Целевое |
|---------|----|-------|---------|
| Шарды | 1 | 10-20 | 10-20 ✓ |
| Max RPS на шард | 5 000 | 2 500 (50 000 / 20) | 2 500 ✓ |
| Latency (p99) | 10 сек | 200-500 мс | <500 мс ✓ |
| CPU | 95%+ | 60-70% | 60-70% ✓ |
| Error rate | 5-10% | <0.1% | <0.1% ✓ |

---

## 2. API Gateway: Stateless Replicas + Auto-scaling

### Текущее состояние

| Параметр | Значение |
|----------|----------|
| Реплики | 1 (bottleneck) |
| Max RPS | ~5 000 |
| CPU | 95%+ |
| Latency (p99) | 5 секунд |

### Реализация горизонтального масштабирования

#### Стратегия: Stateless Replicas + Kubernetes HPA

```
API Gateway Cluster:

Load Balancer (NGINX / HAProxy)
       │
       ├── API Gateway Rep 1
       ├── API Gateway Rep 2
       ├── API Gateway Rep 3
       ├── ...
       └── API Gateway Rep N (auto-scaled)

Auto-scaling triggers (Kubernetes HPA):
  - CPU > 70%
  - RPS > 8,000
  - Min replicas: 10
  - Max replicas: 100

Session Storage (Redis Cluster):
  ├── Redis Primary (Master)
  ├── Redis Replica 1
  └── Redis Replica 2
  Все реплики Gateway читают/пишут сессию из Redis
```

#### Конфигурация

| Параметр | Значение |
|----------|----------|
| Load Balancer | NGINX / HAProxy (L7 routing) |
| Auto-scaling trigger | CPU >70% ИЛИ RPS >8000 |
| Min replicas | 10 (baseline для 500 пользователей) |
| Max replicas | 100 (peak capacity для 50 000) |
| Session storage | Redis Cluster (3 nodes, RF=3) |
| Health check interval | 10 секунд |
| Unhealthy threshold | 3 failed checks → remove из LB |
| Healthy threshold | 2 success checks → add в LB |

#### Результаты масштабирования

| Метрика | До | После | Целевое |
|---------|----|-------|---------|
| Реплики | 1 | 10-100 (auto) | 50-100 ✓ |
| Max RPS | ~5 000 | 500 000+ | 50 000+ ✓ |
| Latency (p99) | 5 сек | 100-200 мс | <500 мс ✓ |
| CPU | 95%+ | 60-70% | 60-70% ✓ |
| Error rate | 5-10% | <0.1% | <0.1% ✓ |

---

## Итоговая таблица масштабирования

| Система | Тип масштабирования | Начальная конфигурация | Целевая конфигурация | Max RPS | Инструменты |
|---------|---------------------|----------------------|---------------------|---------|-------------|
| **API Gateway** | Horizontal (stateless) | 1 replica | 10-100 replicas | 500 000+ | NGINX, K8s HPA, Redis Cluster |
| **Schedule Service** | Sharding (data partitioning) | 1 shard (1 DB) | 10-20 shards (10-20 DBs) | 50 000 | Consistent hashing, PG replicas, PgBouncer |

## Метрики успешного масштабирования

| Метрика | До | После | Целевое |
|---------|----|-------|---------|
| API Gateway latency (p99) | 5 сек | 100-200 мс | <500 мс ✓ |
| Schedule Service latency (p99) | 10 сек | 200-500 мс | <500 мс ✓ |
| Overall System RPS | ~50 RPS | 50 000+ RPS | 50 000+ ✓ |
| CPU utilization | 95%+ | 60-70% | 60-70% ✓ |
| Error rate | 5-10% | <0.1% | <0.1% ✓ |
