"""create product_category

Revision ID: f42bc6a3acec
Revises: 
Create Date: 2021-10-08 11:27:21.514146

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import INTEGER, VARCHAR, NVARCHAR, Column


# revision identifiers, used by Alembic.
revision = 'f42bc6a3acec'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'product_category',
        Column('id', INTEGER, primary_key=True),
        Column('product_id', INTEGER, nullable=False),
        Column('category_id',INTEGER, nullable=False)
    )
    op.create_unique_constraint("uidx_product_category", "product_category", ["product_id", "category_id"])
    op.create_foreign_key("fk_product_category_product", "product_category", "product", ["product_id"], ["id"])
    op.create_foreign_key("fk_product_category_category", "product_category", "category", ["category_id"], ["id"])

def downgrade():
    op.drop_constraint("uidx_product_category", "product_category")
    op.drop_constraint("fk_product_category_product", "product_category", "foreignkey")
    op.drop_constraint("fk_product_category_product", "product_category", "foreignkey")
    op.drop_table("product_category")
