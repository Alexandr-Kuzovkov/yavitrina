"""add tag column

Revision ID: 54c02f683b79
Revises: 8a67bb664f9a
Create Date: 2022-01-24 14:16:27.928664

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '54c02f683b79'
down_revision = '8a67bb664f9a'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE tag ADD COLUMN target_title VARCHAR(255)")


def downgrade():
    pass
