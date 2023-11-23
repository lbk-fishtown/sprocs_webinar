{{
    config(
        post_hook="{{ sp_tpch_parts() }}",
        description = "Lite migration of my tpch_stored_proc",
        tags = ['tpch']
    )
}}

select 1 as sp_lite