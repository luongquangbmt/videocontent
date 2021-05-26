  # Count numbers of subjects, person, tag
  # number of subject
SELECT
  content_id,
  cnt_subject,
  cnt_person,
  cnt_tag,
  subject_coding,
  person_coding,
  tag_coding,
  * EXCEPT (content_id,
    cnt_subject,
    cnt_person,
    cnt_tag,
    subject_coding,
    person_coding,
    tag_coding)
FROM (
  SELECT
    *
  FROM (
    SELECT
      content_id,
      COUNT(subject) AS cnt_subject
    FROM (
      SELECT
        DISTINCT content_id,
        subject
      FROM (
        SELECT
          content_id,
          SPLIT(REGEXP_REPLACE(content_subject,r'[\[\]]', ''), ',') AS subject
        FROM
          `business-intelligence-es.test.kibana`),
        UNNEST(subject) AS subject
      WITH
      OFFSET
        pos_l1 )
    GROUP BY
      content_id
    ORDER BY
      cnt_subject DESC)
  FULL JOIN
    # number of peson
    (
    SELECT
      content_id,
      COUNT(person) AS cnt_person
    FROM (
      SELECT
        DISTINCT content_id,
        person
      FROM (
        SELECT
          content_id,
          SPLIT(REGEXP_REPLACE(content_person,r'[\[\]]', ''), ',') AS person
        FROM
          `business-intelligence-es.test.kibana`),
        UNNEST(person) AS person
      WITH
      OFFSET
        pos_l1 )
    GROUP BY
      content_id
    ORDER BY
      cnt_person DESC)
  USING
    (content_id)
  FULL JOIN
    # number of tag
    (
    SELECT
      content_id,
      COUNT(tag) AS cnt_tag
    FROM (
      SELECT
        DISTINCT content_id,
        tag
      FROM (
        SELECT
          content_id,
          SPLIT(REGEXP_REPLACE(content_tag,r'[\[\]]', ''), ',') AS tag
        FROM
          `business-intelligence-es.test.kibana`),
        UNNEST(tag) AS tag
      WITH
      OFFSET
        pos_l1 )
    GROUP BY
      content_id
    ORDER BY
      cnt_tag DESC)
  USING
    (content_id)
  FULL JOIN (
    SELECT
      *
    FROM (
      SELECT
        CONCAT(CAST(sub_gov AS string), CAST(sub_general AS string),CAST(sub_sport AS string),CAST(sub_business AS string),CAST(sub_lifestyle AS string), CAST(sub_social AS string),CAST(sub_health AS string),CAST(sub_art AS string),CAST(sub_environ AS string),CAST(sub_living AS string),CAST(sub_events AS string),CAST(sub_technology AS string),CAST(sub_media AS string),CAST(sub_demographic AS string),CAST(sub_science AS string)) AS subject_coding,
        *
      FROM (
        SELECT
          content_id,
          MAX(
          IF
            (subject='Government and politics',
              1,
              0))sub_gov,
          MAX(
          IF
            (subject='General news',
              1,
              0))sub_general,
          MAX(
          IF
            (subject='Sports',
              1,
              0))sub_sport,
          MAX(
          IF
            (subject='Business',
              1,
              0))sub_business,
          MAX(
          IF
            (subject='Lifestyle',
              1,
              0))sub_lifestyle,
          MAX(
          IF
            (subject='Social affairs',
              1,
              0))sub_social,
          MAX(
          IF
            (subject='Health',
              1,
              0))sub_health,
          MAX(
          IF
            (subject='Arts and entertainment',
              1,
              0))sub_art,
          MAX(
          IF
            (subject='Environment and nature',
              1,
              0))sub_environ,
          MAX(
          IF
            (subject='Living things',
              1,
              0))sub_living,
          MAX(
          IF
            (subject='Events',
              1,
              0))sub_events,
          MAX(
          IF
            (subject='Technology',
              1,
              0))sub_technology,
          MAX(
          IF
            (subject='Media',
              1,
              0))sub_media,
          MAX(
          IF
            (subject='Demographic groups',
              1,
              0))sub_demographic,
          MAX(
          IF
            (subject='Science',
              1,
              0))sub_science
        FROM (
          SELECT
            content_id,
            subject,
            subject_l1,
            subject_l2
          FROM (
            SELECT
              content_id,
              SPLIT(REGEXP_REPLACE(subject,r'"',''), '/') AS subject,
              subject_l1
            FROM (
              SELECT
                DISTINCT content_id,
                subject,
                subject_l1
              FROM (
                SELECT
                  content_id,
                  SPLIT(REGEXP_REPLACE(content_subject,r'[\[\]]', ''), ',') AS subject
                FROM
                  `business-intelligence-es.test.kibana`),
                UNNEST(subject) AS subject
              WITH
              OFFSET
                subject_l1 )),
            UNNEST(subject) AS subject
          WITH
          OFFSET
            subject_l2
            #where subject_l2 = 0
            )
        GROUP BY
          content_id )))
  USING
    (content_id)
  FULL JOIN (
    SELECT
      CONCAT(CAST(per_Trudeau AS string),CAST(per_Trump AS string),CAST(per_Scheer AS string),CAST(per_Singh AS string),CAST(per_Kenney AS string),CAST(per_May AS string),CAST(per_Ford AS string),CAST(per_Pallister AS string),CAST(per_Harper AS string),CAST(per_Bernier AS string),CAST(per_Thunberg AS string),CAST(per_Johnson AS string),CAST(per_Black AS string),CAST(per_Queen AS string),CAST(per_Moe AS string),CAST(per_Bilden AS string),CAST(per_Atwood AS string),CAST(per_Roger AS string),CAST(per_Legault AS string),CAST(per_Andreescu AS string),CAST(per_Lynch AS string),CAST(per_Raybould AS string),CAST(per_Watson AS string),CAST(per_Higgs AS string),CAST(per_Lam AS string),CAST(per_Aaron AS string),CAST(per_Island AS string),CAST(per_Freeland AS string),CAST(per_horgan AS string)) AS person_coding,
      *
    FROM (
      SELECT
        content_id,
        MAX(
        IF
          (person='Justin Trudeau',
            1,
            0))per_Trudeau,
        MAX(
        IF
          (person='Donald Trump',
            1,
            0))per_Trump,
        MAX(
        IF
          (person='Andrew Scheer',
            1,
            0))per_Scheer,
        MAX(
        IF
          (person='Jagmeet Singh',
            1,
            0))per_Singh,
        MAX(
        IF
          (person='Jason Kenney',
            1,
            0))per_Kenney,
        MAX(
        IF
          (person='Elizabeth May',
            1,
            0))per_May,
        MAX(
        IF
          (person='Doug Ford',
            1,
            0))per_Ford,
        MAX(
        IF
          (person='Brian Pallister',
            1,
            0))per_Pallister,
        MAX(
        IF
          (person='Stephen Harper',
            1,
            0))per_Harper,
        MAX(
        IF
          (person='Maxime Bernier',
            1,
            0))per_Bernier,
        MAX(
        IF
          (person='Greta Thunberg"',
            1,
            0))per_Thunberg,
        MAX(
        IF
          (person='Boris Johnson',
            1,
            0))per_Johnson,
        MAX(
        IF
          (person='"Ian Black',
            1,
            0))per_Black,
        MAX(
        IF
          (person='Queen Elizabeth II',
            1,
            0))per_Queen,
        MAX(
        IF
          (person='Scott Moe',
            1,
            0))per_Moe,
        MAX(
        IF
          (person='Joe Biden',
            1,
            0))per_Bilden,
        MAX(
        IF
          (person='Margaret Atwood',
            1,
            0))per_Atwood,
        MAX(
        IF
          (person='Roger',
            1,
            0))per_Roger,
        MAX(
        IF
          (person='François Legault',
            1,
            0))per_Legault,
        MAX(
        IF
          (person='Bianca Andreescu',
            1,
            0))per_Andreescu,
        MAX(
        IF
          (person='Laura Lynch',
            1,
            0))per_Lynch,
        MAX(
        IF
          (person='Jody Wilson-Raybould',
            1,
            0))per_Raybould,
        MAX(
        IF
          (person='Jim Watson',
            1,
            0))per_Watson,
        MAX(
        IF
          (person='Blaine Higgs',
            1,
            0))per_Higgs,
        MAX(
        IF
          (person='Carrie Lam',
            1,
            0))per_Lam,
        MAX(
        IF
          (person='Aaron',
            1,
            0))per_Aaron,
        MAX(
        IF
          (person='"Edward Island',
            1,
            0))per_Island,
        MAX(
        IF
          (person='Chrystia Freeland',
            1,
            0))per_Freeland,
        MAX(
        IF
          (person='John Horgan',
            1,
            0))per_horgan
      FROM (
        SELECT
          content_id,
          person
        FROM (
          SELECT
            content_id,
            SPLIT(REGEXP_REPLACE(content_person,r'[\[\]"]', ''), ',') AS person
          FROM
            `business-intelligence-es.test.kibana`),
          UNNEST(person) AS person
        WITH
        OFFSET
          person_l1 )
      GROUP BY
        content_id ))
  USING
    (content_id)
  FULL JOIN (
    SELECT
      CONCAT(CAST(tag_city AS string),CAST(tag_people AS string),CAST(tag_new AS string),CAST(tag_canadavote AS string),CAST(tag_school AS string),CAST(tag_man AS string),CAST(tag_goverment AS string),CAST(tag_year AS string),CAST(tag_RCMP AS string),CAST(tag_CBCArchives AS string),CAST(tag_indigenous AS string),CAST(tag_psft AS string),CAST(tag_students AS string)) AS tag_coding,
      *
    FROM (
      SELECT
        content_id,
        MAX(
        IF
          (tag='city',
            1,
            0))tag_city,
        MAX(
        IF
          (tag='people',
            1,
            0))tag_people,
        MAX(
        IF
          (tag='new',
            1,
            0))tag_new,
        MAX(
        IF
          (tag='Canada Votes 2019',
            1,
            0))tag_canadavote,
        MAX(
        IF
          (tag='school',
            1,
            0))tag_school,
        MAX(
        IF
          (tag='man',
            1,
            0))tag_man,
        MAX(
        IF
          (tag='goverment',
            1,
            0))tag_goverment,
        MAX(
        IF
          (tag='year',
            1,
            0))tag_year,
        MAX(
        IF
          (tag='RCMP',
            1,
            0))tag_RCMP,
        MAX(
        IF
          (tag='CBC Archives',
            1,
            0))tag_CBCArchives,
        MAX(
        IF
          (tag='indigenous',
            1,
            0))tag_indigenous,
        MAX(
        IF
          (tag='ps_ft',
            1,
            0))tag_psft,
        MAX(
        IF
          (tag='students',
            1,
            0))tag_students
      FROM (
        SELECT
          content_id,
          tag
        FROM (
          SELECT
            content_id,
            SPLIT(REGEXP_REPLACE(content_tag,r'[\[\]"]', ''), ',') AS tag
          FROM
            `business-intelligence-es.test.kibana`),
          UNNEST(tag) AS tag
        WITH
        OFFSET
          tag_l1 )
      GROUP BY
        content_id ))
  USING
    (content_id)
  ORDER BY
    person_coding DESC )
