import asyncio
import asyncpg


async def wait_for_db(params: dict,
                      max_retries: int = 10,
                      delay: int = 5) -> None:
    retries = 0
    while retries < max_retries:
        try:
            conn = await asyncpg.connect(**params)
            await conn.close()
            print("Подключение к базе данных установлено!")
            return
        except ConnectionRefusedError:
            print(f"База данных вероятно ещё не готова. Повторная попытка через {delay} секунд...")
            retries += 1
            await asyncio.sleep(delay)
    raise Exception("Не удалось подключиться к базе данных.")


async def change_value(id: str,
                       value: str,
                       sign: str,
                       conn: asyncpg.Connection) -> None:
    await conn.execute(f"""
    UPDATE accounts
    SET balance = balance {sign} {value}
    WHERE account_id = {id};
    """)


async def transfer_money(sender: str,
                         getter: str,
                         value: str,
                         conn: asyncpg.Connection,
                         test_ser_level: bool = False,
                         add_note_to_transfers: bool = False) -> None:
    if test_ser_level:
        params = dict(isolation='serializable')
    else:
        params = dict()

    async with conn.transaction(**params):

        await change_value(id=sender,
                           value=value,
                           sign="-",
                           conn=conn)
        await change_value(id=getter,
                           value=value,
                           sign="+",
                           conn=conn)
        if add_note_to_transfers:
            await conn.execute(f"""
            INSERT INTO transfers (from_account_id, to_account_id, amount)
            VALUES ({sender}, {getter}, {value})
            """)


async def check_that_balance_is_same(conn: asyncpg.Connection) -> None:
    query = """
    SELECT array_agg(balance::int ORDER BY account_id) AS balances
    FROM accounts;
    """
    result = await conn.fetchrow(query)

    balances = result['balances']
    assert balances == [1000, 1500, 2000]


async def main() -> None:
    params = dict(
        user='test_user',
        password='aboba',
        database='test',
        host='postgres',  # По имени контейнера
        port=5432
    )

    await wait_for_db(params, 5, 10)

    conn1 = await asyncpg.connect(**params)
    conn2 = await asyncpg.connect(**params)

    # 1 задача
    try:
        async with conn1.transaction():
            value = str(200)
            await change_value("1", value, "-", conn1)
            await conn1.execute("aaaaa")
            # Прокидываем синтаксический бред
            await change_value("2", value, "+", conn1)

    except asyncpg.exceptions.PostgresSyntaxError as e:
        print(f"Ошибка в 1 задании {e}")
        await check_that_balance_is_same(conn1)

    # 2 задача
    try:
        async with conn1.transaction():
            value = str(5000)
            await transfer_money("1", "3", value, conn1)
    except asyncpg.exceptions.CheckViolationError as e:
        print(f"Ошибка в 2 задании {e}")
        await check_that_balance_is_same(conn1)

    # 3 задача
    try:
        task1 = asyncio.create_task(transfer_money("1", "2", "200", conn1, test_ser_level=True))
        task2 = asyncio.create_task(transfer_money("2", "1", "200", conn2, test_ser_level=True))
        await task1
        await task2

    except asyncpg.exceptions.SerializationError as e:
        print(f"Ошибка в 3 задаче: {e}")

        try:
            await check_that_balance_is_same(conn1)
            print("В задаче три были выполнены обе транзакции")
        except AssertionError:
            print("В задаче три была выполнена только одна транзакция")

    # 4 задача
    try:
        await conn1.execute("BEGIN")
        await transfer_money("2", "1", "200", conn2)
        await conn1.execute("SAVEPOINT my_savepoint")
        await conn1.execute("ZAPROZ S OSHIBKOI")

    except asyncpg.exceptions.PostgresSyntaxError:
        await conn1.execute("ROLLBACK TO SAVEPOINT my_savepoint")
        await conn1.execute("COMMIT")

        # Баланс должен был вернуться к стартовому
        await check_that_balance_is_same(conn1)
        print("В задаче 4 сработал savepoint")

    # 5 задача
    try:
        task1 = asyncio.create_task(transfer_money("1", "2", "200", conn1))
        task2 = asyncio.create_task(transfer_money("2", "1", "200", conn2))
        await task1
        await task2
    except asyncpg.exceptions.DeadlockDetectedError:
        try:
            await check_that_balance_is_same(conn1)
            print("Выполнились обе операции, вызвавшие дедлок")
        except AssertionError:
            print("Выполнилась лишь одна из операций, вызвавших дедлок")

    await conn1.execute("""
    CREATE TABLE transfers (
    id SERIAL PRIMARY KEY,
    from_account_id INTEGER REFERENCES accounts(account_id),
    to_account_id INTEGER REFERENCES accounts(account_id),
    amount DECIMAL(10, 2)
    );
    """)

    await transfer_money("1", "2", "200", conn1, add_note_to_transfers=True)

    query = """
    SELECT ARRAY[from_account_id, to_account_id, amount]
    FROM transfers;
    """
    rows = await conn1.fetch(query)
    assert all(el1 == int(el2) for el1, el2 in zip([1, 2, 200], rows[0]["array"]))

    await conn1.close()
    await conn2.close()


asyncio.run(main())
