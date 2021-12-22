"""add product colors

Revision ID: 02b5c2281186
Revises: e0ed50b6ebd4
Create Date: 2021-12-01 19:15:08.193021

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '02b5c2281186'
down_revision = 'e0ed50b6ebd4'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE product ADD COLUMN colors VARCHAR(255)")


def downgrade():
    pass
