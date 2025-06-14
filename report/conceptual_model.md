# Conceptual Data Model

## Entity-Relationship Diagram

**Relationship**: One-to-Many (1:N)
- One PRODUCT can have many REVIEWS
- One REVIEW belongs to exactly one PRODUCT
- Foreign Key: REVIEW.product_id → PRODUCT.id

**Business Rules**:
- Products must have unique IDs
- Reviews must reference existing products
- Ratings constrained to 1-5 scale
- Review dates auto-generated