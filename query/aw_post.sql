SELECT
    p.url_key,
    p.title,
    p.content,
    p.short_content,
    p.publish_date,
    p.status,
    p.featured_image_file,
    p.featured_image_alt,
    p.meta_title,
    p.meta_description,
    CONCAT(
        ba.firstname,
        ' ',
        ba.lastname
    ) AS author_full_name
FROM
    aw_blog_post AS p
    JOIN aw_blog_post_author AS bpa ON p.id = bpa.post_id
    JOIN aw_blog_author AS ba ON bpa.author_id = ba.id
WHERE
    ba.id = 14;