"""alter image column

Revision ID: 094f52efcef6
Revises: e00d9e3fcfab
Create Date: 2021-09-01 19:41:42.572425

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '094f52efcef6'
down_revision = 'e00d9e3fcfab'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('image', 'product_id', nullable=True)


def downgrade():
    op.alter_column('image', 'product_id', nullable=False)
