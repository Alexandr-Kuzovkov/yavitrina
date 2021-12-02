"""add product related_products

Revision ID: 07e068fbe8f7
Revises: 63ec01ab2280
Create Date: 2021-12-02 17:52:26.330961

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '07e068fbe8f7'
down_revision = '63ec01ab2280'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE product ADD COLUMN related_products TEXT")

def downgrade():
    pass
