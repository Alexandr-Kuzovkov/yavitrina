"""add image column

Revision ID: e00d9e3fcfab
Revises: b4fc285f0ab9
Create Date: 2021-09-01 19:13:44.082005

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e00d9e3fcfab'
down_revision = 'b4fc285f0ab9'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('image', sa.Column('category_url', sa.String))


def downgrade():
    op.drop_column('image', 'category_url')
