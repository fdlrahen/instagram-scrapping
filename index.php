<?php
require __DIR__ . '/config.php';

// Ambil semua data dari tabel ig_posts
// Kamu bisa batasi pakai LIMIT kalau takut kelamaan
$sql = "SELECT id, shortcode, username, caption, date_utc, event_date,
               is_video, url, sidecars, permalink
        FROM ig_posts
        ORDER BY date_utc DESC
        LIMIT 200";

$stmt = $pdo->query($sql);
$posts = $stmt->fetchAll();
?>
<!doctype html>
<html lang="en">
<head>
    <title>Daftar Event Instagram</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no"/>

    <link
        href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css"
        rel="stylesheet"
        integrity="sha384-T3c6CoIi6uLrA9TneNEoa7RxnatzjcDSCmG1MXxSR1GAsXEV/Dwwykc2MPK8M2HN"
        crossorigin="anonymous"
    />
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css">
</head>

<body>
<header>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
        <div class="container">
            <a class="navbar-brand" href="#">Event Instagram</a>
            <button class="navbar-toggler" type="button" data-bs-toggle="collapse"
                    data-bs-target="#navbarNav" aria-controls="navbarNav"
                    aria-expanded="false" aria-label="Toggle navigation">
                <span class="navbar-toggler-icon"></span>
            </button>
        </div>
    </nav>
</header>

<main class="py-4">
    <div class="container">
        <div class="mb-3">
            <h1 class="h3">Daftar Event Calendar</h1>
            <p class="text-muted mb-0">Data dari tabel <code>ig_posts</code></p>
        </div>
        <a href="create.php" class="btn btn-success btn-sm mb-3">
            <i class="bi bi-plus-lg"></i> Tambah Event
        </a>

        <br>

        <div class="table-responsive">
            <table class="table table-striped table-sm align-middle">
                <thead class="table-dark">
                <tr>
                    <th>No</th>
                    <th>Shortcode</th>
                    <th>Username</th>
                    <th>Caption</th>
                    <th>Date UTC</th>
                    <th>Event Date</th>
                    <th>Video?</th>
                    <th>URL</th>
                    <th>Sidecars</th>
                    <th>Permalink</th>
                    <th>Edit</th>
                    <th>Delete</th>
                </tr>
                </thead>
                <tbody>
                <?php if (empty($posts)): ?>
                    <tr>
                        <td colspan="12" class="text-center text-muted">
                            Belum ada data di tabel <code>ig_posts</code>.
                        </td>
                    </tr>
                <?php else: ?>
                    <?php foreach ($posts as $i => $row): ?>
                        <tr>
                            <td><?= $i + 1 ?></td>
                            <td><?= htmlspecialchars($row['shortcode']) ?></td>
                            <td><?= htmlspecialchars($row['username']) ?></td>
                            <td style="max-width: 260px;">
                                <div class="small text-truncate" style="max-width: 260px;">
                                    <?= htmlspecialchars($row['caption']) ?>
                                </div>
                            </td>
                            <td>
                                <?= htmlspecialchars($row['date_utc']) ?>
                            </td>
                            <td><?= htmlspecialchars($row['event_date']) ?></td>
                            <td><?= $row['is_video'] ? 'Ya' : 'Tidak' ?></td>
                            <td>
                                <?php if (!empty($row['url'])): ?>
                                    <a href="<?= htmlspecialchars($row['url']) ?>" target="_blank">Link</a>
                                <?php endif; ?>
                            </td>
                            <td>
                                <?php
                                $countSidecars = 0;
                                if (!empty($row['sidecars'])) {
                                    $decoded = json_decode($row['sidecars'], true);
                                    if (is_array($decoded)) {
                                        $countSidecars = count($decoded);
                                    }
                                }
                                ?>
                                <?= $countSidecars ? $countSidecars . ' media' : '-' ?>
                            </td>
                            <td>
                                <?php if (!empty($row['permalink'])): ?>
                                    <a href="<?= htmlspecialchars($row['permalink']) ?>" target="_blank">
                                        View IG
                                    </a>
                                <?php endif; ?>
                            </td>
                            <td>
                                <a href="edit.php?id=<?= (int)$row['id'] ?>"
                                   class="btn btn-sm btn-warning">Edit</a>
                            </td>
                            <td>
                                <a href="delete.php?id=<?= (int)$row['id'] ?>"
                                   class="btn btn-sm btn-danger"
                                   onclick="return confirm('Yakin hapus data ini?');">
                                    Delete
                                </a>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                <?php endif; ?>
                </tbody>
            </table>
        </div>
    </div>
</main>

<script
    src="https://cdn.jsdelivr.net/npm/@popperjs/core@2.11.8/dist/umd/popper.min.js"
    crossorigin="anonymous"
></script>
<script
    src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.min.js"
    crossorigin="anonymous"
></script>
</body>
</html>
