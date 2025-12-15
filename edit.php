<?php
require __DIR__ . '/config.php';

// Bisa kamu DRY bareng create.php, tapi di sini aku tulis ulang saja
function indo_to_utc($text) {
    $bulan = [
        'januari'=>'01','februari'=>'02','maret'=>'03','april'=>'04','mei'=>'05','juni'=>'06',
        'juli'=>'07','agustus'=>'08','september'=>'09','oktober'=>'10','november'=>'11','desember'=>'12'
    ];
    $text = strtolower(trim($text));
    $parts = explode(' ', $text);

    if (count($parts) === 3) {
        $d = $parts[0];
        $m = $bulan[$parts[1]] ?? null;
        $y = $parts[2];
        if ($m) {
            return sprintf('%04d-%02d-%02d 00:00:00', (int)$y, (int)$m, (int)$d);
        }
    }
    return null;
}

$id = isset($_GET['id']) ? (int)$_GET['id'] : 0;
if ($id <= 0) {
    die('ID tidak valid');
}

// Ambil data lama
$stmt = $pdo->prepare("SELECT * FROM ig_posts WHERE id = :id");
$stmt->execute(['id' => $id]);
$post = $stmt->fetch();

if (!$post) {
    die('Data tidak ditemukan');
}

// Kalau form disubmit
if ($_SERVER['REQUEST_METHOD'] === 'POST') {

    // Kalau mau, shortcode & permalink juga bisa diedit dari form
    $shortcode  = $_POST['shortcode']  ?? $post['shortcode'];
    $username   = $_POST['username']   ?? '';
    $caption    = $_POST['caption']    ?? '';
    $event_date = $_POST['event_date'] ?? '';
    $permalink  = $_POST['permalink']  ?? $post['permalink'];
    $gas_status = $_POST['gas_status'] ?? ($post['gas_status'] ?? '');

    $date_utc   = indo_to_utc($event_date);
    $send_now   = isset($_POST['send_now']);

    // --- 1) UPDATE DB dulu ---
    $sql = "
        UPDATE ig_posts
        SET
            shortcode  = :shortcode,
            username   = :username,
            caption    = :caption,
            event_date = :event_date,
            date_utc   = :date_utc,
            permalink  = :permalink,
            gas_status = :gas_status,
            " . ($send_now ? "gas_sent_at = NULL," : "") . "
            updated_at = NOW()
        WHERE id = :id
    ";

    $update = $pdo->prepare($sql);
    $update->execute([
        'shortcode'  => $shortcode,
        'username'   => $username,
        'caption'    => $caption,
        'event_date' => $event_date,
        'date_utc'   => $date_utc,
        'permalink'  => $permalink,
        'gas_status' => $gas_status,
        'id'         => $id,
    ]);

    // --- 2) Kalau tidak kirim ke GAS, cukup sampai sini ---
    if (!$send_now) {
        header('Location: index.php?status=updated');
        exit;
    }

    // --- 3) Siapkan payload ke GAS (format sama dengan create.php) ---
    $date_epoch = $date_utc ? strtotime($date_utc) : null;

    $record = [
        'shortcode'  => $shortcode,
        'username'   => $username,
        'caption'    => $caption,
        'event_date' => $event_date,
        'date_utc'   => $date_epoch,
        'permalink'  => $permalink,
        // kalau di DB kamu ada gcal_event_id dan createEventFromRecord_ bisa handle update,
        // bisa sekalian dikirim:
        // 'gcal_event_id' => $post['gcal_event_id'] ?? null,
    ];

    $payload = [
        'secret'  => GAS_SECRET,
        'records' => [ $record ],
        // kalau perlu beda mode, bisa tambahkan:
        // 'mode' => 'update',
    ];

    $ch = curl_init(GAS_WEBAPP_URL);
    curl_setopt($ch, CURLOPT_POST, true);
    curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($payload));
    curl_setopt($ch, CURLOPT_HTTPHEADER, ['Content-Type: application/json']);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);

    $result   = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $err      = curl_error($ch);
    curl_close($ch);

    // --- 4) Error handling dasar ---
    if ($result === false || $err) {
        header("Location: index.php?status=error&msg=" . urlencode("CURL_ERROR: $err"));
        exit;
    }

    if ($httpCode !== 200) {
        header("Location: index.php?status=error&msg=" . urlencode("HTTP_$httpCode RESP: $result"));
        exit;
    }

    $json = json_decode($result, true);
    if (!$json || empty($json['ok'])) {
        header("Location: index.php?status=error&msg=" . urlencode("INVALID_RESPONSE: $result"));
        exit;
    }

    // kalau cuma kirim 1 record, cek results[0]
    $first = $json['results'][0] ?? null;
    if (empty($first) || empty($first['ok'])) {
        $errMsg = $first['error'] ?? 'Unknown GAS error';
        header("Location: index.php?status=error&msg=" . urlencode("GAS_ERROR: $errMsg"));
        exit;
    }

    // --- 5) Update gas_sent_at & gas_status di DB ---
    $upd = $pdo->prepare(
        "UPDATE ig_posts
         SET gas_sent_at = NOW(), gas_status = 'SENT'
         WHERE id = :id"
    );
    $upd->execute(['id' => $id]);

    header("Location: index.php?status=sent");
    exit;
}

// kalau GET biasa, tinggal render form pakai $post
?>
<!doctype html>
<html lang="en">
<head>
    <title>Edit Event</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link
        href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css"
        rel="stylesheet"
    />
</head>
<body class="bg-light">
<div class="container py-4">
    <h1 class="h4 mb-3">Edit Event Instagram</h1>

    <form method="post" class="card p-3">
        <div class="mb-3">
            <label class="form-label">Shortcode</label>
            <input type="text" name="shortcode" class="form-control"
                   value="<?= htmlspecialchars($post['shortcode'] ?? '') ?>">
        </div>

        <div class="mb-3">
            <label class="form-label">Nama Kegiatan (username)</label>
            <input type="text" name="username" class="form-control"
                   value="<?= htmlspecialchars($post['username'] ?? '') ?>">
        </div>

        <div class="mb-3">
            <label class="form-label">Caption</label>
            <input type="text" name="caption" class="form-control"
                   value="<?= htmlspecialchars($post['caption'] ?? '') ?>">
        </div>

        <div class="mb-3">
            <label class="form-label">Event Date (teks)</label>
            <input type="text" name="event_date" class="form-control"
                   placeholder="contoh: 30 November 2025"
                   value="<?= htmlspecialchars($post['event_date'] ?? '') ?>">
        </div>

        <div class="mb-3">
            <label class="form-label">Permalink</label>
            <input type="text" name="permalink" class="form-control"
                   value="<?= htmlspecialchars($post['permalink'] ?? '') ?>">
        </div>

        <div class="mb-3">
            <label class="form-label">GAS Status</label>
            <input type="text" name="gas_status" class="form-control"
                   placeholder="misal: SENT / PENDING"
                   value="<?= htmlspecialchars($post['gas_status'] ?? '') ?>">
        </div>

        <div class="mb-3 form-check">
            <input type="checkbox" name="send_now" value="1" class="form-check-input" id="sendNow">
            <label class="form-check-label" for="sendNow">
                Kirim ulang ke Google Calendar sekarang
            </label>
        </div>

        <div class="d-flex gap-2">
            <button type="submit" class="btn btn-primary">Simpan</button>
            <a href="index.php" class="btn btn-secondary">Kembali</a>
        </div>
    </form>
</div>
</body>
</html>
