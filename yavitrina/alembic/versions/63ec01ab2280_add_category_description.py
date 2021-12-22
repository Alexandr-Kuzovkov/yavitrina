"""add category description

Revision ID: 63ec01ab2280
Revises: 02b5c2281186
Create Date: 2021-12-01 19:18:53.287196

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '63ec01ab2280'
down_revision = '02b5c2281186'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE category ADD COLUMN description TEXT")


def downgrade():
    pass
