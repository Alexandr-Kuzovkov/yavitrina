"""add rate column

Revision ID: e0ed50b6ebd4
Revises: c972d4b3c2c8
Create Date: 2021-12-01 17:30:08.623677

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e0ed50b6ebd4'
down_revision = 'c972d4b3c2c8'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE product ADD COLUMN rate VARCHAR(255)")


def downgrade():
    pass
