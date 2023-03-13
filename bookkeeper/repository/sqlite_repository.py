"""
Модуль описывает репозиторий, работающий в оперативной памяти
"""

from itertools import count
from typing import Any, List
import sqlite3
from bookkeeper.repository.abstract_repository import AbstractRepository, T


class SqliteRepository(AbstractRepository[T]):


    def __init__(self) -> None:
            self.conn = sqlite3.connect("base")
            self.cursor = self.conn.cursor()
            self.cursor.execute('''CREATE TABLE IF NOT EXISTS objects
                            (pk INTEGER PRIMARY KEY, data TEXT)''')
            
    def add(self, obj: T) -> int:
        # Проверяем, что у объекта нет атрибута pk или он равен None
        if not hasattr(obj, 'pk') or obj.pk is None:
            # Генерируем новый уникальный идентификатор
            self.cursor.execute("INSERT INTO objects (data) VALUES (?)", (str(obj),))
            last_row_id = self.cursor.lastrowid
            obj.pk = int(last_row_id) if last_row_id is not None else 0
        else:
            # Обновляем данные объекта с заданным идентификатором
            self.cursor.execute("UPDATE objects SET data = ? WHERE pk = ?", (str(obj), obj.pk))
        self.conn.commit()
        return obj.pk
    
    def get(self, pk: int) -> T | None:
        self.cursor.execute("SELECT data FROM objects WHERE pk = ?", (pk,))
        result = self.cursor.fetchone()
        if result:
            res :T = result[0]
            return res
        else:
            return None
    
    def get_all(self, where: dict[str, Any] | None = None)  -> list[T]:
        res: List[T] = []
        if where is None:
            self.cursor.execute("SELECT data FROM objects")
            results = self.cursor.fetchall()
            for result in results:
                res.append(result)
            return res
        else:#надо поменять на нормальную реализацию
            self.cursor.execute("SELECT data FROM objects")
            results = self.cursor.fetchall()
            for result in results:
                res.append(result)
            return res
             
    
    def update(self, obj: T) -> None:
        # Обновляем данные объекта с заданным идентификатором
        self.cursor.execute("UPDATE objects SET data = ? WHERE pk = ?", (str(obj), obj.pk))
        self.conn.commit()
    
    def delete(self, pk: int) -> None:
        # Удаляем объект с заданным идентификатором
        self.cursor.execute("DELETE FROM objects WHERE pk = ?", (pk,))
        self.conn.commit()
