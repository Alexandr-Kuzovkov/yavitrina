"""add category column

Revision ID: b4fc285f0ab9
Revises: 
Create Date: 2021-09-01 19:02:02.901122

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b4fc285f0ab9'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('category', sa.Column('img', sa.String))


def downgrade():
    op.drop_column('category', 'img')
