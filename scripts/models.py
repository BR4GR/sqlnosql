from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

@dataclass
class Product:
    id: int
    name: str
    category: str
    price: float
    description: str

@dataclass
class Review:
    id: int
    product_id: int
    user_name: str
    rating: int
    review_text: str
    date: datetime