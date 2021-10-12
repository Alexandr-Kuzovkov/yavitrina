"""create search_tag

Revision ID: bae22e18573e
Revises: 71125e4ca796
Create Date: 2021-10-12 15:47:47.319032

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'bae22e18573e'
down_revision = '71125e4ca796'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('''CREATE TABLE IF NOT EXISTS search_tag
                    (
                        id serial not null constraint search_tag_pk primary key,
                        url VARCHAR(255) NOT NULL,
                        html TEXT,
                        created_at timestamp with time zone default now() not null,
                        title VARCHAR(255) NOT NULL,
                        updated_at timestamp with time zone,
                        page TEXT
                    )''')
    op.execute('ALTER TABLE search_tag OWNER TO "vitrina"')
    op.execute('CREATE UNIQUE INDEX uidx_search_tag_title on search_tag (title)')

    op.execute('''CREATE TABLE IF NOT EXISTS category_tag
                        (
                            id serial not null constraint category_tag_pk primary key,
                            url VARCHAR(255) NOT NULL,
                            html TEXT,
                            created_at timestamp with time zone default now() not null,
                            title VARCHAR(255) NOT NULL,
                            updated_at timestamp with time zone,
                            page TEXT
                        )''')
    op.execute('ALTER TABLE category_tag OWNER TO "vitrina"')
    op.execute('CREATE UNIQUE INDEX uidx_category_tag_title on category_tag (title)')



def downgrade():
   op.execute('DROP INDEX uidx_search_tag_title')
   op.execute('DROP TABLE search_tag')
   op.execute('DROP INDEX uidx_category_tag_title')
   op.execute('DROP TABLE caregory_tag')