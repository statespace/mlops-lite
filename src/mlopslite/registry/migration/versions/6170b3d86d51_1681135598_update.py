"""1681135598_update

Revision ID: 6170b3d86d51
Revises: 
Create Date: 2023-04-10 17:06:38.845071

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6170b3d86d51'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('data_registry',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('name', sa.String(), nullable=False),
    sa.Column('version', sa.Integer(), nullable=False),
    sa.Column('description', sa.String(), nullable=False),
    sa.Column('data', sa.JSON(), nullable=False),
    sa.Column('size_cols', sa.Integer(), nullable=False),
    sa.Column('size_rows', sa.Integer(), nullable=False),
    sa.Column('hash', sa.String(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('hash'),
    sa.UniqueConstraint('name', 'version')
    )
    op.create_table('data_registry_columns',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('data_registry_id', sa.Integer(), nullable=False),
    sa.Column('column_name', sa.String(), nullable=False),
    sa.Column('original_dtype', sa.String(), nullable=False),
    sa.Column('converted_dtype', sa.String(), nullable=False),
    sa.Column('null_count', sa.Integer(), nullable=False),
    sa.Column('unique_count', sa.Integer(), nullable=False),
    sa.Column('min_value_num', sa.Float(), nullable=True),
    sa.Column('max_value_num', sa.Float(), nullable=True),
    sa.ForeignKeyConstraint(['data_registry_id'], ['data_registry.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('data_registry_columns')
    op.drop_table('data_registry')
    # ### end Alembic commands ###
