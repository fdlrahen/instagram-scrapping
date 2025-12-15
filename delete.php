<?php
require __DIR__ . '/config.php';

$id = isset($_GET['id']) ? (int)$_GET['id'] : 0;
if ($id <= 0) {
    die('ID tidak valid');
}

$del = $pdo->prepare("DELETE FROM ig_posts WHERE id = :id");
$del->execute(['id' => $id]);

header('Location: index.php');
exit;
