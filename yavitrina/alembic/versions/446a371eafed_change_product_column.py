"""change product column

Revision ID: 446a371eafed
Revises: bae22e18573e
Create Date: 2021-11-08 16:29:33.236903

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '446a371eafed'
down_revision = 'bae22e18573e'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('UPDATE product SET parameters=NULL WHERE parameters NOTNULL')
    op.execute('ALTER TABLE product ALTER COLUMN parameters TYPE jsonb USING parameters::jsonb')


def downgrade():
    pass
